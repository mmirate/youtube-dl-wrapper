use anyhow::{anyhow, bail, Context, Result};
use base64::URL_SAFE_NO_PAD;
use chrono::prelude::*;
use crossbeam_utils::atomic::AtomicCell;
use derivative::Derivative;
use duct::cmd;
use extension_trait::extension_trait;
use itertools::Itertools;
use kuchiki::traits::*;
#[allow(unused_imports)]
use log::{debug, error, info, warn, LevelFilter};
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rayon::prelude::*;
use serde::{self, Deserialize, Serialize};
use serde_json::{self, Value};
// use serde_bytes;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

use crate::aria2;

static HTTP_CLIENT: once_cell::sync::Lazy<reqwest::blocking::Client> = once_cell::sync::Lazy::new(Default::default);

#[extension_trait]
impl OsStrExt for OsStr {
    fn try_to_str(&self) -> Result<&str> {
        self.to_str().with_context(|| format!("not utf-8: {:?}", self))
    }
}
#[extension_trait(pub)]
impl PathExt for Path {
    fn try_to_str(&self) -> Result<&str> {
        self.to_str().with_context(|| format!("not utf-8: {:?}", self))
    }
}
#[extension_trait(pub)]
impl PathBufExt for PathBuf {
    fn try_into_string(self) -> Result<String> {
        self.into_os_string().into_string().map_err(|e| anyhow!("not utf-8: {:?}", e))
    }
}
#[extension_trait]
impl OsStringExt for OsString {
    fn try_into_string(self) -> Result<String> {
        self.into_string().map_err(|e| anyhow!("not utf-8: {:?}", e))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum StartingPointTag {
    Channel(Option<String>),
    Playlist,
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct StartingPoint<'a> {
    pub(crate) url: Cow<'a, str>,
    pub(crate) id: &'a str,
    path: PathBuf,
    tag: StartingPointTag,
}
impl<'a> StartingPoint<'a> {
    pub(crate) fn new(url: &'a str) -> Result<Self> {
        static URL_RES: once_cell::sync::Lazy<Vec<regex::Regex>> = once_cell::sync::Lazy::new(|| {
            [r#".+?/(?P<typ>channel)/(?P<id>[^/]+)/playlists\b.*"#, r#".+?/(?P<typ>playlist)\?list=(?P<id>[^/]+)"#]
                .iter()
                .copied()
                .map(regex::Regex::new)
                .map(Result::unwrap)
                .collect()
        });
        let (typ, id) = URL_RES
            .par_iter()
            .find_map_first(|re| re.captures(url))
            .and_then(|cs| cs.name("typ").into_iter().zip(cs.name("id")).next())
            .map(|c| (c.0.as_str(), c.1.as_str()))
            .with_context(|| format!("invalid url: {:?}", url))?;
        let url: Cow<str> = url.into();
        let shelf_id = reqwest::Url::parse(&url)?
            .query_pairs()
            .collect::<BTreeMap<_, _>>()
            .remove("shelf_id")
            .map(Cow::into_owned);
        Ok(match typ {
            "channel" => StartingPoint {
                url,
                id,
                path: Path::new(id).with_extension("json"),
                tag: StartingPointTag::Channel(shelf_id),
            },
            "playlist" => {
                StartingPoint { url, id, path: Path::new(id).with_extension("json"), tag: StartingPointTag::Playlist }
            }
            x => bail!("invalid url: typ={:?}", x),
        })
    }
    pub(crate) fn read(&self) -> Result<Option<YoutubePlaylists>> {
        info!("parsing {:?}", &self.path);
        match std::fs::File::open(&self.path) {
            Ok(rdr) => Ok({
                let mut ret = match &self.tag {
                    StartingPointTag::Channel(_) => {
                        serde_json::from_reader(rdr).with_context(|| format!("cannot parse {:?}", &self.path))?
                    }
                    StartingPointTag::Playlist => YoutubePlaylists::singleton_from_playlist(
                        serde_json::from_reader(rdr).with_context(|| format!("cannot parse {:?}", &self.path))?,
                    ),
                };
                /*let out_path = self.path.with_extension("out.json");
                let bytes = serde_json::to_vec(&ret)?;
                duct::cmd("jq", &["--sort-keys", "."])
                    .stdout_path(out_path)
                    .stdin_bytes(bytes)
                    .run()?;*/
                if ret.read_progress()? == Progress::MAX {
                    None
                } else {
                    for pl in &mut ret.entries {
                        pl.sp_id = self.id.to_owned();
                        for item in &mut pl.entries {
                            if let Some(item) = item {
                                item.sp_id = self.id.to_owned();
                            }
                        }
                    }
                    Some(ret)
                }
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) fn amend(&self, new_value: &YoutubePlaylists) -> Result<()> {
        info!("writing back {}", &self.url);
        serde_json::to_writer_pretty(std::fs::File::create(&self.path)?, new_value).map_err(From::from)
    }
    #[allow(clippy::block_in_if_condition_stmt)]
    pub(crate) fn scrape(
        &mut self, pending_changes_tx: crossbeam_channel::Sender<String>, use_cookies: bool,
    ) -> Result<()> {
        if let StartingPointTag::Channel(None) = self.tag {
            info!("need shelf for {:?}", self);
            let mut base = reqwest::Url::parse(&self.url)?;
            base.query_pairs_mut().append_pair("disable_polymer", "true");
            let text = {
                let mut resp: reqwest::blocking::Response = HTTP_CLIENT.get(base.clone()).send()?.error_for_status()?;
                let mut url = resp.url().clone();
                if url.path_segments().and_then(|o| o.last()) != Some("playlists") {
                    url.path_segments_mut()
                        .map_err(|_| anyhow!("cannot-be-a-base http url: {}", resp.url().as_str()))?
                        .extend(&["playlists"]);
                    resp = HTTP_CLIENT.get(url).send()?.error_for_status()?;
                }
                resp.text()
            }?;
            let doc = kuchiki::parse_html().one(text.as_str());
            if let Some(correct_url) =
                doc.select("*[href*=\"shelf_id=\"]").map_err(|()| anyhow!("CSS selector problem"))?.find_map(|e| {
                    if let Some(e) = e.as_node().as_element() {
                        if let Some(href) = e.attributes.borrow().get("href") {
                            if let Ok(url) = base.join(href) {
                                if let Some(shelf_id) = url.query_pairs().collect::<BTreeMap<_, _>>().get("shelf_id") {
                                    if shelf_id != "0" {
                                        info!("Comparing {}", &url.as_str());
                                        info!(" ... with {}", self.url);
                                        let other = StartingPoint::new(&url.as_str());
                                        if let Ok(other) = other {
                                            if self.id == other.id {
                                                return Some(url.into_string());
                                            } else {
                                                error!("{:?} != {:?}", self, other);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None
                })
            {
                let rhs = correct_url.clone();
                let lhs = std::mem::replace(&mut self.url, correct_url.into()).into_owned();
                info!("found shelf: {} => {}", lhs, rhs);
                pending_changes_tx.send(format!(
                    "s#\\b{}$#{}#;\n",
                    regex_syntax::escape(&lhs),
                    regex_syntax::escape(&rhs)
                ))?;
            } else {
                bail!("couldn't find shelf for {:?} inside {}", self, text);
            }
        }
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&self.path) {
            Ok(out_file) => {
                info!("scraping {:?}", self);
                youtube_dl_to(&self.url, out_file, use_cookies)?
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => info!("already scraped {:?}", self),
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}

fn youtube_dl_(url: &str, use_cookies: bool) -> duct::Expression {
    let mut args = if use_cookies {
        vec![
            "--cookies",
            "cookies.txt",
            "--user-agent",
            "Mozilla/5.0 (X11; Linux armv7l; rv:75.0) Gecko/20100101 Firefox/75.0",
        ]
    } else {
        vec![]
    };
    args.extend_from_slice(&[
        "-f",
        "bestaudio[protocol!=http_dash_segments]",
        "--youtube-skip-dash-manifest",
        "-Ji",
        url,
    ]);
    let ret = cmd("youtube-dl", args).pipe(cmd!("jq", "--sort-keys", "."));
    info!("Running {:?}", ret);
    ret
}

fn youtube_dl_to(url: &str, out_file: std::fs::File, use_cookies: bool) -> Result<()> {
    Ok(youtube_dl_(url, use_cookies).stdout_file(out_file).unchecked().run().map(drop)?)
}

fn youtube_dl(url: &str, use_cookies: bool) -> Result<duct::ReaderHandle> {
    Ok(youtube_dl_(url, use_cookies).reader()?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemId(pub u64);
impl ToString for ItemId {
    fn to_string(&self) -> String {
        let input: [u8; 8] = self.0.to_ne_bytes();
        let mut output: [u8; 11] = Default::default();
        base64::encode_config_slice(&input, URL_SAFE_NO_PAD, &mut output);
        String::from_utf8_lossy(&output).into_owned()
    }
}
impl From<aria2::Gid> for ItemId {
    fn from(x: aria2::Gid) -> Self {
        ItemId(x.0)
    }
}
impl<'de> Deserialize<'de> for ItemId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        use serde::de::Error;
        let mut output: [u8; 8] = Default::default();
        let input: String = Deserialize::deserialize(deserializer)?;
        // let input: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        if input.len() != 11 {
            return Err(D::Error::custom("length of `id` must be 11"));
        } //*/
        base64::decode_config_slice(&input, URL_SAFE_NO_PAD, &mut output).map_err(D::Error::custom)?;
        Ok(ItemId(u64::from_ne_bytes(output)))
    }
}
impl Serialize for ItemId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::Error;
        let input: [u8; 8] = self.0.to_ne_bytes();
        let mut output: [u8; 11] = Default::default();
        base64::encode_config_slice(&input, URL_SAFE_NO_PAD, &mut output);
        // serde_bytes::serialize(&output[..], serializer)
        serializer.serialize_str(std::str::from_utf8(&output).map_err(S::Error::custom)?)
    }
}

fn deserialize_ac<'de, D, T: Deserialize<'de> + Copy>(deserializer: D) -> Result<AtomicCell<T>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input: T = Deserialize::deserialize(deserializer)?;
        Ok(AtomicCell::new(input))
    }


    fn serialize_ac<S, T: Serialize + Copy>(output: &AtomicCell<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let output: T = output.load();
        output.serialize(serializer)
    }

/*#[serde(flatten)] extra: HashMap<String, Value>,*/

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct UploaderDetails {
    uploader: String,
    uploader_id: String,
    uploader_url: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
struct DiscriminantDetails {
    extractor: String,
    extractor_key: String,
    webpage_url_basename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    _type: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Progress {
    Unknown,
    Interrupted,
    Queued,
    Downloaded,
    Postprocessed,
}
impl Progress {
    const MAX: Progress = Progress::Postprocessed;
}
impl Default for Progress {
    fn default() -> Self {
        Progress::Unknown
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FormatDetails {
    acodec: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    container: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    downloader_options: HashMap<String, Value>,
    ext: String,
    filesize: Option<u64>,
    http_headers: HashMap<String, String>,
    protocol: String,
    url: String,
    #[serde(default)]
    url_expiry: Option<DateTime<Utc>>,
    // video
    fps: Option<u32>,
    height: Option<u32>,
    width: Option<u32>,
    vcodec: String,
    tbr: f64,
    // nomenclature
    format: String,
    format_id: String,
    format_note: String,
    player_url: Option<String>,
    // audiophily
    #[serde(skip_serializing_if = "Option::is_none")]
    abr: Option<u32>,
    asr: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetItemExt {
    age_limit: usize,
    annotations: Value,
    automatic_captions: Value,
    average_rating: Value,
    channel_id: String,
    channel_url: String,
    categories: Value,
    chapters: Value,
    dislike_count: Option<usize>,
    like_count: Option<usize>,
    display_id: String,
    duration: usize,
    end_time: Value,
    episode_number: Value,
    is_live: Option<bool>,
    license: Value,
    playlist_index: Option<usize>,
    n_entries: Option<usize>,
    playlist_title: Option<String>,
    playlist_uploader: Option<String>,
    playlist_uploader_id: Option<String>,
    release_date: Option<String>,
    release_year: Option<usize>,
    requested_subtitles: Value,
    season_number: Value,
    series: Value,
    start_time: Value,
    subtitles: Value,
    tags: Vec<String>,
    thumbnail: String,
    thumbnails: Value,
    upload_date: String,
    view_count: usize,
}

#[derive(Derivative, Deserialize, Serialize)]
#[derivative(Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetItem {
    pub(crate) id: ItemId,
    title: String,
    track: Option<String>,
    webpage_url: String,
    #[derivative(Debug = "ignore")]
    formats: Vec<FormatDetails>,
    playlist: Option<String>,
    playlist_id: Option<String>,
    #[derivative(Debug = "ignore")]
    album: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[derivative(Debug = "ignore")]
    album_artist: Option<String>,
    #[derivative(Debug = "ignore")]
    alt_title: Option<String>,
    #[derivative(Debug = "ignore")]
    artist: Option<String>,
    #[derivative(Debug = "ignore")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[derivative(Debug = "ignore")]
    track_number: Option<usize>,
    #[derivative(Debug = "ignore")]
    creator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[derivative(Debug = "ignore")]
    genre: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[derivative(Debug = "ignore")]
    disc_number: Option<usize>,
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    progress: AtomicCell<Progress>,
    #[serde(flatten)]
    #[derivative(Debug = "ignore")]
    irrelevant: Box<TargetItemExt>,
    #[serde(flatten)]
    #[derivative(Debug = "ignore")]
    chosen_format: RwLock<FormatDetails>,
    #[serde(flatten)]
    discriminant: DiscriminantDetails,
    #[serde(flatten)]
    uploader: UploaderDetails,
    #[serde(skip)]
    sp_id: String,
    #[serde(default)] #[serde(serialize_with="serialize_ac", deserialize_with="deserialize_ac")] pub(crate) dupe: AtomicCell<bool>,
}
impl Eq for TargetItem where String: Eq {}
impl PartialEq for TargetItem {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl Ord for TargetItem
where
    String: Eq,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for TargetItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
pub(crate) enum TargetItemQueueDownloadResult {
    AlreadyDownloaded,
    Queued,
    QueuedButMustAmend,
}
pub(crate) use TargetItemQueueDownloadResult::*;
impl TargetItem {
    fn filename_with_extension(&self, extension: impl AsRef<OsStr>) -> PathBuf {
        let mut ret = PathBuf::from(self.id.to_string());
        ret.set_extension(extension);
        ret
    }
    fn filename(&self) -> PathBuf {
        self.filename_with_extension(&self.chosen_format.read().ext)
    }
    fn filename_with_added_extension(&self, extension: impl AsRef<str>) -> PathBuf {
        let mut ret = PathBuf::from(self.id.to_string());
        let ext = self.chosen_format.read().ext.clone() + "." + extension.as_ref();
        ret.set_extension(ext);
        ret
    }
    pub(crate) fn read_progress(&self) -> Result<Progress> {
        {
            let progress = self.progress.load();
            if progress != Progress::Unknown {
                return Ok(progress);
            }
        }
        let status_path = &self.filename_with_extension("status.json");
        let aria2_path = self.filename_with_added_extension("aria2");
        self.progress.store(match std::fs::File::open(&status_path) {
            Ok(rdr) => {
                let ret: Progress = serde_json::from_reader(rdr)?;
                if ret == Progress::Queued && aria2_path.is_file() {
                    // std::fs::remove_file(status_path)?;
                    Progress::Interrupted
                } else {
                    ret
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let ret = if aria2_path.is_file() { Progress::Interrupted } else { Default::default() };
                serde_json::to_writer(std::fs::File::create(&status_path)?, &ret)?;
                ret
            }
            Err(e) => return Err(e.into()),
        });
        Ok(self.progress.load())
    }
    pub(crate) fn write_progress(&self, new_value: Progress) -> Result<()> {
        self.progress.store(new_value);
        let path = &self.filename_with_extension("status.json");
        serde_json::to_writer(std::fs::File::create(&path)?, &new_value)?;
        Ok(())
    }
    fn _refresh_if_needed<'s, 'g>(&'s self, cfg: &mut RwLockWriteGuard<'g, FormatDetails>) -> Result<()> {
        use std::convert::TryInto;
        static CC_RE: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| regex::Regex::new(r#"max-age=([0-9]+)"#).unwrap());
        cfg.url_expiry = if let Some(query_expiry) = reqwest::Url::parse(&cfg.url)?
            .query_pairs()
            .collect::<BTreeMap<_, _>>()
            .remove("expire")
            .and_then(|s| s.parse().ok())
        {
            Some(Utc.timestamp(query_expiry, 0) - chrono::Duration::minutes(30))
        } else {
            warn!("running HEAD manually for {}", self.id.to_string());
            let response = HTTP_CLIENT
                .head(&cfg.url)
                .headers(
                    cfg.http_headers
                        .iter()
                        .map(|(k, v)| Ok((k.try_into()?, v.as_str().try_into()?)))
                        .collect::<Result<_>>()?,
                )
                .send()?;
            response
                .headers()
                .get_all(reqwest::header::CACHE_CONTROL)
                .into_iter()
                .filter_map(|v| v.to_str().ok())
                .find_map(|v| CC_RE.captures(v))
                .map(|cs| -> Result<_> {
                    Ok(Utc::now() - chrono::Duration::minutes(30) + chrono::Duration::seconds(cs[1].parse()?))
                })
                .transpose()?
        };
        Ok(())
    }
    pub(crate) fn refresh_if_needed<'s, 'g>(
        &'s self, cfg: RwLockUpgradableReadGuard<'g, FormatDetails>, use_cookies: bool,
    ) -> Result<(Option<String>, RwLockUpgradableReadGuard<'g, FormatDetails>)> {
        Ok(if cfg.url_expiry < Some(Utc::now()) {
            use std::ops::DerefMut;
            let mut cfg = RwLockUpgradableReadGuard::upgrade(cfg);
            // now doublecheck since someone else could've got here first
            if cfg.url_expiry < Some(Utc::now()) {
                if let Some(query_expiry) = reqwest::Url::parse(&cfg.url)?
                    .query_pairs()
                    .collect::<BTreeMap<_, _>>()
                    .remove("expire")
                    .and_then(|s| s.parse().ok())
                {
                    let query_expiry = Utc.timestamp(query_expiry, 0);
                    if query_expiry > Utc::now() {
                        cfg.url_expiry = Some(query_expiry);
                        (None, RwLockWriteGuard::downgrade_to_upgradable(cfg))
                    } else if let Some(other) = serde_json::from_reader(youtube_dl(&self.webpage_url, use_cookies).context("youtube_dl callsite failed")?).context("serde_json failed")? {
                        let other: Self = other;
                        let old_url = std::mem::replace(cfg.deref_mut(), other.chosen_format.into_inner()).url;
                        self._refresh_if_needed(&mut cfg)?;
                        (Some(old_url), RwLockWriteGuard::downgrade_to_upgradable(cfg))
                    } else {
                        (None, RwLockWriteGuard::downgrade_to_upgradable(cfg))
                    }
                } else if let Some(other) =
                    serde_json::from_reader(youtube_dl(&self.webpage_url, use_cookies).context("youtube_dl callsite failed")?).context("serde_json failed")?
                {
                    let other: Self = other;
                    let old_url = std::mem::replace(cfg.deref_mut(), other.chosen_format.into_inner()).url;
                    self._refresh_if_needed(&mut cfg)?;
                    (Some(old_url), RwLockWriteGuard::downgrade_to_upgradable(cfg))
                } else {
                    (None, RwLockWriteGuard::downgrade_to_upgradable(cfg))
                }
            } else {
                (None, RwLockWriteGuard::downgrade_to_upgradable(cfg))
            }
        } else {
            (None, cfg)
        })
    }
    pub(crate) fn requeue_if_needed(&self, use_cookies: bool) -> Result<bool> {
        let cfg = self.chosen_format.upgradable_read();
        if let (Some(old_url), cfg) = self.refresh_if_needed(cfg, use_cookies)? {
            aria2::ChangeUriParams::new(self.id.into(), &old_url, &cfg.url).send()?;
            return Ok(true);
        }
        Ok(false)
    }
    pub(crate) fn queue_download(
        &self, client: &reqwest::blocking::Client, use_cookies: bool,
    ) -> Result<TargetItemQueueDownloadResult> {
        {
            let progress = self.read_progress()?;
            if progress >= Progress::Queued {
                info!("not queueing {} since it is {:?}", self.id.to_string(), progress);
                return Ok(AlreadyDownloaded);
            }
        }
        info!("queueing {}", self.id.to_string());
        /*match (std::fs::metadata(self.filename()), self.chosen_format.filesize) {
            (Err(e), _) if e.kind() == std::io::ErrorKind::NotFound => {},
            (Err(e), _) => Err(e)?,
            (Ok(m), maybe_len) if !m.is_file() || maybe_len != Some(m.len()) => {},
            (Ok(_), _) => return Ok(false),
        }*/
        let (old_url, cfg) = self.refresh_if_needed(self.chosen_format.upgradable_read(), use_cookies)?;
        let headers =
            self.chosen_format.read().http_headers.iter().map(|(k, v)| format!("{}: {}", k, v)).collect::<Vec<_>>();
        aria2::AddUriParams::new(
            &cfg.url,
            aria2::AddUriOptions {
                dir: std::env::current_dir()?,
                gid: self.id.into(),
                header: headers.iter().map(String::as_str).collect(),
                out: &self.filename().try_into_string()?,
            },
        )
        .send_with(client)?;
        self.write_progress(Progress::Queued)?;
        drop(cfg);
        info!("queued {}", self.id.to_string());
        Ok(if old_url.is_some() { QueuedButMustAmend } else { Queued })
    }
    pub(crate) fn poll_all_downloads() -> Result<(Vec<aria2::Download>, Vec<aria2::Download>, Vec<aria2::Download>)> {
        Ok((
            aria2::TellWaitingParams::new().send()?.contents.result,
            aria2::TellActiveParams::new().send()?.contents.result,
            aria2::TellStoppedParams::new().send()?.contents.result,
        ))
    }
    #[allow(dead_code)]
    fn poll_is_download_complete(&self, use_cookies: bool) -> Result<bool> {
        let progress = self.progress.load();
        if progress < Progress::Queued {
            return Ok(false);
        } else if progress >= Progress::Downloaded {
            return Ok(true);
        }
        let gid = self.id.into();
        let result = aria2::TellStatusParams::new(gid).send()?.contents.result;
        use aria2::DownloadStatus::*;
        Ok(match result.status {
            Complete | Removed => {
                aria2::RemoveResultParams::new(gid).send()?;
                self.write_progress(Progress::Downloaded)?;
                true
            }
            Error => bail!("download of {} failed: {}", self.id.to_string(), result.error_message),
            Waiting | Paused => {
                self.requeue_if_needed(use_cookies)?;
                false
            }
            Active => false,
        })
    }
    #[allow(dead_code)]
    fn block_until_download_completion(&self, use_cookies: bool) -> Result<()> {
        if self.progress.load() >= Progress::Downloaded {
            return Ok(());
        }
        let json = aria2::TellStatusParams::new(self.id.into());
        loop {
            use aria2::DownloadStatus::*;
            let result = json.send()?.contents.result;
            match result.status {
                Complete | Removed => {
                    aria2::RemoveResultParams::new(self.id.into()).send()?;
                    self.write_progress(Progress::Downloaded)?;
                    return Ok(());
                }
                Error => bail!("download of {} failed: {}", self.id.to_string(), result.error_message),
                Waiting | Paused => {
                    self.requeue_if_needed(use_cookies)?;
                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
                Active => std::thread::sleep(std::time::Duration::from_secs(5)),
            }
        }
    }
    pub(crate) fn postprocess(&self) -> Result<()> {
        {
            let progress = self.progress.load();
            if progress >= Progress::Postprocessed || progress < Progress::Downloaded {
                return Ok(());
            }
        }
        info!("postprocessing {}", self.id.to_string());
        static DESIRED_EXT: &str = "flac";
        static DESIRED_ACODEC: &str = "flac";
        let input_filename = format!("file:{}", self.filename().try_into_string()?);
        let output_filename = format!("file:{}", &self.filename_with_extension(DESIRED_EXT).try_into_string()?);
        if input_filename == output_filename {
            return Ok(());
        }
        let input_acodec: String = {
            static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
                regex::Regex::new(r#"(?m)^codec_name=(.+)$(?:.|\n)+?^codec_type=audio$"#).unwrap()
            });
            let buf = cmd(
                "ffprobe",
                &[
                    //"-nostats",
                    //"-nostdin",
                    "-loglevel",
                    "level+warning",
                    "-hide_banner",
                    "-show_streams",
                    &input_filename,
                ],
            )
            .stdout_capture()
            .read()?;
            RE.captures(&buf)
                .map(|cs| cs[1].to_owned())
                .with_context(|| format!("Cannot probe codec for {:?}", &self.id.to_string()))?
        };
        let output_acodec = if input_acodec == DESIRED_ACODEC { "copy" } else { DESIRED_ACODEC };

        let (metadata, metadata_count) = {
            let mut i = 0usize;
            let mut metadata = String::new();
            metadata += "-metadata\0title=";
            metadata += self.track.as_ref().unwrap_or(&self.title);
            metadata += "\0";
            i += 2;
            if let Some(description) = self.description.as_ref() {
                metadata += "-metadata\0description=";
                metadata += description;
                metadata += "\0";
                i += 2;
            }
            metadata += "-metadata\0purl=";
            metadata += &self.webpage_url;
            metadata += "\0";
            i += 2;
            if let Some(track) = self.track_number.as_ref() {
                metadata += "-metadata\0track=";
                metadata += &track.to_string();
                metadata += "\0";
                i += 2;
            }
            metadata += "-metadata\0artist=";
            metadata += self
                .artist
                .as_ref()
                .or_else(|| self.creator.as_ref())
                .unwrap_or(&self.uploader.uploader)
                .trim_end_matches(" - Topic");
            metadata += "\0";
            i += 2;
            if let Some(genre) = self.genre.as_ref() {
                metadata += "-metadata\0genre=";
                metadata += genre;
                metadata += "\0";
                i += 2;
            }
            if let Some(album) = self.album.as_ref() {
                metadata += "-metadata\0album=";
                metadata += album;
                metadata += "\0";
                i += 2;
            }
            if let Some(album_artist) = self.album_artist.as_ref() {
                metadata += "-metadata\0album_artist=";
                metadata += album_artist.trim_end_matches(" - Topic");
                metadata += "\0";
                i += 2;
            }
            if let Some(disc_number) = self.disc_number.as_ref() {
                metadata += "-metadata\0disc=";
                metadata += &disc_number.to_string();
                metadata += "\0";
                i += 2;
            }
            (metadata, i)
        };

        let mut ffmpeg_args = Vec::with_capacity(3 + metadata_count + 4);
        ffmpeg_args.extend_from_slice(&[
            "-y",
            "-nostats",
            "-nostdin",
            "-loglevel",
            "level+warning",
            "-hide_banner",
            "-i",
            &input_filename,
        ]);
        ffmpeg_args.extend(metadata.split_terminator('\0'));
        ffmpeg_args.extend_from_slice(&["-vn", "-acodec", output_acodec, &output_filename]);

        cmd("ffmpeg", ffmpeg_args).run()?;
        self.write_progress(Progress::Postprocessed)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct YoutubePlaylist {
    pub(crate) entries: Vec<Option<TargetItem>>,
    title: String,
    id: String,
    webpage_url: String,
    #[serde(flatten)]
    uploader: Option<UploaderDetails>,
    #[serde(flatten)]
    discriminant: DiscriminantDetails,
    #[serde(default)] #[serde(serialize_with="serialize_ac", deserialize_with="deserialize_ac")] pub(crate) dupe: AtomicCell<bool>,
    #[serde(skip)]
    sp_id: String,
}
impl Eq for YoutubePlaylist {}
impl PartialEq for YoutubePlaylist {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl Ord for YoutubePlaylist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for YoutubePlaylist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl YoutubePlaylist {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn read_progress(&self) -> Result<Progress> {
        self.entries
            .par_iter()
            .flatten()
            .map(TargetItem::read_progress)
            .reduce(|| Ok(Progress::MAX), |x, y| Ok(std::cmp::min(x?, y?)))
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct YoutubePlaylists {
    pub(crate) entries: Vec<YoutubePlaylist>,
    title: String,
    id: String,
    webpage_url: String,
    #[serde(flatten)]
    uploader: Option<UploaderDetails>,
    #[serde(flatten)]
    discriminant: DiscriminantDetails,
}
impl YoutubePlaylists {
    pub(crate) fn len(&self) -> usize {
        self.entries.iter().map(YoutubePlaylist::len).sum()
    }
    pub(crate) fn read_progress(&self) -> Result<Progress> {
        self.entries
            .par_iter()
            .map(YoutubePlaylist::read_progress)
            .reduce(|| Ok(Progress::MAX), |x, y| Ok(std::cmp::min(x?, y?)))
    }
    fn singleton_from_playlist(playlist: YoutubePlaylist) -> Self {
        YoutubePlaylists { entries: vec![playlist], ..Default::default() }
    }
    pub(crate) fn find_dupes<'a>(input: impl IntoParallelIterator<Item = &'a Self>) -> Result<std::collections::BTreeSet<String>> {
        let mut ret = vec![];
        let mut playlists =
            input.into_par_iter().flat_map(|ps| ps.entries.par_iter()).collect::<Vec<&YoutubePlaylist>>();
        playlists.par_sort_by_key(|t: &&YoutubePlaylist| &*t.id);
        let dupe_playlists = find_dupes_impl(
            playlists.iter().copied(),
            |t: &&YoutubePlaylist| &*t.id,
            |t| t.entries.iter().flatten().count(),
            |t| !t.dupe.load()
        )?;
        for pl in dupe_playlists {
            pl.dupe.store(true);
            ret.push(pl.sp_id.clone());
        }

        let mut items = playlists
            .into_par_iter()
            .flat_map(|p: &YoutubePlaylist| p.entries.par_iter().flatten())
            .collect::<Vec<&TargetItem>>();
        items.par_sort_by_key(|t: &&TargetItem| t.id);
        let dupe_items = find_dupes_impl(
            items,
            |t: &&TargetItem| t.id,
            |t| (t.progress.load(), Some(&t.uploader.uploader) == t.irrelevant.playlist_uploader.as_ref(), t.chosen_format.read().tbr.round() as u32),
            |t| !t.dupe.load())?;
        for item in dupe_items {
            item.dupe.store(true);
            ret.push(item.sp_id.clone());
        }
        Ok(ret.into_iter().collect())
    }
}

fn find_dupes_impl<T: std::fmt::Debug, K: Ord + std::fmt::Debug, L: Ord + std::fmt::Debug>(
    input: impl IntoIterator<Item = T>, key: impl Fn(&T) -> K, fitness_key: impl Fn(&T) -> L + Copy, validity_key: impl Fn(&T) -> bool + Copy,
) -> Result<Vec<T>> {
    let mut dupe_buckets: Vec<(K, Vec<T>)> = input
        .into_iter()
        .group_by(key)
        .into_iter()
        .filter_map(|(k, g)| Some((k, g.filter(validity_key).collect_vec())).filter(|(_k, g)| g.len() > 1))
        .collect();
    let mut ret = vec![];
    for (_k, bucket) in &mut dupe_buckets {
        bucket.sort_by_key(fitness_key);
        ret.extend(bucket.drain(..bucket.len()-1));
    }
    Ok(ret)
}
