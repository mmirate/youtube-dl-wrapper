use anyhow::{anyhow, bail, Context, Result};
use base64::URL_SAFE_NO_PAD;
use chrono::prelude::*;
use crossbeam_utils::atomic::AtomicCell;
use derivative::*;  // star-import is to appease rust-analyzer
use duct::cmd;
use extension_trait::extension_trait;
use kuchiki::traits::*;
#[allow(unused_imports)]
use log::{debug, error, info, warn, LevelFilter};
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use rayon::prelude::*;
use serde::{self, Deserialize, Serialize};
// use serde_bytes;
use std::borrow::Cow;
use std::collections::BTreeMap;
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
#[derive(Debug)]
//#[derivative(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StartingPoint<'a> {
    /*#[derivative(Hash="ignore", PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]*/
    pub(crate) url: Cow<'a, str>,
    pub(crate) id: &'a str,
    /*#[derivative(Hash="ignore", PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]*/ path: PathBuf,
    /*#[derivative(Hash="ignore", PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]*/
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
    pub(crate) fn read(&mut self, rescrape: bool, use_cookies: bool, postprocess: bool) -> Result<Vec<TargetItem>> {
        self.scrape(rescrape, use_cookies)?;
        info!("parsing {:?}", &self.path);
        match std::fs::File::open(&self.path) {
            Ok(rdr) => Ok({
                let mut pls = match &self.tag {
                    StartingPointTag::Channel(_) => {
                        serde_json::from_reader(rdr).with_context(|| format!("cannot parse {:?}", &self.path))?
                    }
                    StartingPointTag::Playlist => YoutubePlaylists::singleton_from_playlist(
                        serde_json::from_reader(rdr).with_context(|| format!("cannot parse {:?}", &self.path))?,
                    ),
                };
                for pl in pls.entries.iter_mut() {
                    for item in pl.entries.iter_mut() {
                        if let Some(old_item) = item.as_mut() {
                            if let Ok(new_item) = old_item.reread() {
                                *old_item = new_item;
                            }
                        }
                    }
                }
                if let Some(progress) = pls.progress() {
                    if progress == Progress::MAX || !postprocess && progress == Progress::Downloaded {
                        info!("already processed {}", self.id);
                        vec![]
                    } else if progress < Progress::Downloaded
                        && !rescrape
                        && pls.expiry() < Some(Utc::now())
                        && !pls.is_empty()
                    {
                        self.read(true, use_cookies, postprocess)?
                    } else {
                        pls.entries
                            .into_par_iter()
                            .flat_map(|pl| pl.entries)
                            .flatten()
                            .map(|t| {
                                t.write()?;
                                Ok(t)
                            })
                            .collect::<Result<_>>()?
                    }
                } else {
                    info!("no items contained in {}", self.id);
                    vec![]
                }
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
            Err(e) => Err(e.into()),
        }
    }
    fn get_shelf(&mut self) -> Result<()> {
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
            if let Some((correct_url, correct_tag)) =
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
                                                if let StartingPointTag::Channel(Some(_)) = other.tag {
                                                    let correct_tag = other.tag;
                                                    return Some((url.into_string(), correct_tag));
                                                } else {
                                                    error!("{:?} has no shelf", other);
                                                }
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
                let lhs = correct_url.clone();
                let rhs = std::mem::replace(&mut self.url, correct_url.into()).into_owned();
                self.tag = correct_tag;
                info!("found shelf: {} => {}", lhs, rhs);
            } else {
                bail!("couldn't find shelf for {:?} inside {}", self, text);
            }
        }
        Ok(())
    }
    #[allow(clippy::block_in_if_condition_stmt)]
    fn scrape(&mut self, rescrape: bool, use_cookies: bool) -> Result<()> {
        match std::fs::OpenOptions::new().write(true).create_new(true).open(&self.path) {
            Ok(out_file) => {
                self.get_shelf()?;
                info!("scraping {:?}", self.url);
                youtube_dl_to_file(&self.url, out_file, use_cookies)?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if rescrape {
                    self.get_shelf()?;
                    info!("rescraping {:?}", self.url);
                    youtube_dl_to_path(&self.url, &self.path, use_cookies)?;
                } else {
                    info!("already scraped {:?}", self.url)
                }
            }
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
        "-4",
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

fn youtube_dl_to_path(url: &str, out_file: &Path, use_cookies: bool) -> Result<()> {
    Ok(youtube_dl_(url, use_cookies).unchecked().pipe(cmd!("ifne", "sponge", out_file)).run().map(drop)?)
}

fn youtube_dl_to_file(url: &str, out_file: std::fs::File, use_cookies: bool) -> Result<()> {
    Ok(youtube_dl_(url, use_cookies).stdout_file(out_file).run().map(drop)?)
}

fn youtube_dl(url: &str, use_cookies: bool) -> Result<duct::ReaderHandle> {
    Ok(youtube_dl_(url, use_cookies).reader()?)
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemId(pub u64);
impl std::str::FromStr for ItemId {
    type Err = anyhow::Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut output: [u8; 8] = Default::default();
        if input.len() != 11 {
            bail!("id must be 11 chars of base64");
        }
        base64::decode_config_slice(input, URL_SAFE_NO_PAD, &mut output)?;
        Ok(ItemId(u64::from_ne_bytes(output)))
    }
}
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct UploaderDetails {
    uploader: String,
    uploader_id: String,
    uploader_url: String,
}

#[derive(Debug, Deserialize, Serialize, Default, PartialEq, Eq, PartialOrd, Ord)]
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
    Remuxed,
    Recoded,
}
impl Progress {
    pub(crate) const MAX: Progress = Progress::Recoded;
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    downloader_options: BTreeMap<String, usize>,
    ext: String,
    filesize: Option<u64>,
    http_headers: BTreeMap<String, String>,
    protocol: String,
    url: String,
    #[serde(default)]
    url_expiry: Option<DateTime<Utc>>,
    // video
    fps: Option<u32>,
    height: Option<u32>,
    width: Option<u32>,
    vcodec: String,
    tbr: f64, //fixed::types::I10F54,
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
    annotations: Option<BTreeMap<String, ()>>,
    automatic_captions: BTreeMap<String, String>,
    average_rating: Option<f64>, //Option<fixed::types::I4F60>,
    channel_id: String,
    channel_url: String,
    categories: Option<Vec<String>>,
    chapters: Option<serde_json::Value>,
    dislike_count: Option<usize>,
    like_count: Option<usize>,
    display_id: String,
    duration: usize,
    end_time: Option<()>,
    episode_number: Option<()>,
    is_live: Option<bool>,
    license: Option<()>,
    playlist_index: Option<usize>,
    n_entries: Option<usize>,
    playlist_title: Option<String>,
    playlist_uploader: Option<String>,
    playlist_uploader_id: Option<String>,
    release_date: Option<String>,
    release_year: Option<usize>,
    requested_subtitles: Option<()>,
    season_number: Option<()>,
    series: Option<()>,
    start_time: Option<()>,
    subtitles: Option<BTreeMap<String, ()>>,
    tags: Vec<String>,
    thumbnail: String,
    thumbnails: Vec<BTreeMap<String, String>>,
    upload_date: String,
    view_count: usize,
}

#[derive(Derivative, Deserialize, Serialize)]
#[derivative(Debug/*, PartialEq, Eq, PartialOrd, Ord*/)]
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
    #[serde(default)]
    #[serde(serialize_with = "serialize_ac", deserialize_with = "deserialize_ac")]
    #[derivative(Debug = "ignore", PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    progress: AtomicCell<Progress>,
    #[serde(flatten)]
    #[derivative(Debug = "ignore")]
    irrelevant: Box<TargetItemExt>,
    #[serde(flatten)]
    #[derivative(Debug = "ignore", PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    chosen_format: RwLock<FormatDetails>,
    #[serde(flatten)]
    discriminant: DiscriminantDetails,
    #[serde(flatten)]
    uploader: UploaderDetails,
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
    fn download_filename(&self) -> PathBuf {
        self.filename_with_extension(&self.chosen_format.read().ext)
    }
    fn info_filename(&self) -> PathBuf {
        self.filename_with_extension("json")
    }
    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        let cfg = self.chosen_format.upgradable_read();
        let cfg = if cfg.url_expiry.is_none() {
            if let Some(query_expiry) = reqwest::Url::parse(&cfg.url).ok().and_then(|url| {
                url.query_pairs().collect::<BTreeMap<_, _>>().remove("expire").and_then(|s| s.parse().ok())
            }) {
                let mut cfg = RwLockUpgradableReadGuard::upgrade(cfg);
                let query_expiry = Utc.timestamp(query_expiry, 0);
                cfg.url_expiry = Some(query_expiry);
                RwLockWriteGuard::downgrade_to_upgradable(cfg)
            } else {
                cfg
            }
        } else {
            cfg
        };
        cfg.url_expiry
    }
    fn reread(&self) -> Result<Self> {
        let path = self.info_filename();
        serde_json::from_reader(std::fs::File::open(path)?).map_err(From::from)
    }
    pub(crate) fn progress(&self) -> Progress { self.progress.load() }
    pub(crate) fn write(&self) -> Result<()> {
        let path = self.info_filename();
        let new_contents = serde_json::to_string_pretty(&self)?;
        match std::fs::read_to_string(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            Ok(old_contents) if old_contents == new_contents => (),
            _ => std::fs::write(path, new_contents)?,
        }
        Ok(())
    }
    pub(crate) fn write_progress(&self, new_value: Progress) -> Result<()> {
        self.progress.store(new_value);
        let path = &self.info_filename();
        serde_json::to_writer_pretty(std::fs::File::create(&path)?, &self)?;
        match std::fs::remove_file(&self.filename_with_extension("status.json")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            x => x,
        }?;
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
                    } else if let Some(other) = serde_json::from_reader(
                        youtube_dl(&self.webpage_url, use_cookies).context("youtube_dl callsite failed")?,
                    )
                    .context("serde_json failed")?
                    {
                        let other: Self = other;
                        let old_url = std::mem::replace(cfg.deref_mut(), other.chosen_format.into_inner()).url;
                        self._refresh_if_needed(&mut cfg)?;
                        (Some(old_url), RwLockWriteGuard::downgrade_to_upgradable(cfg))
                    } else {
                        (None, RwLockWriteGuard::downgrade_to_upgradable(cfg))
                    }
                } else if let Some(other) = serde_json::from_reader(
                    youtube_dl(&self.webpage_url, use_cookies).context("youtube_dl callsite failed")?,
                )
                .context("serde_json failed")?
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
            let progress = self.progress.load();
            if progress > Progress::Queued {
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
                out: &self.download_filename().try_into_string()?,
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
    pub(crate) fn dump_metadata(&self) -> Result<()> {
        {
            let progress = self.progress.load();
            if progress < Progress::Downloaded {
                warn!("tried to dump metadata for {} before it was downloaded, ignoring", self.id.to_string());
                return Ok(());
            }
        }
        info!("dumping metadata for {}", self.id.to_string());
        let input_filename = format!("file:{}", self.download_filename().try_into_string()?);
        let metadata_filename = format!("file:{}", &self.filename_with_extension("ffmetadata").try_into_string()?);
        if input_filename == metadata_filename {
            return Ok(());
        }
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

        let mut ffmpeg_args = Vec::with_capacity(8 + metadata_count + 4);
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
        ffmpeg_args.extend_from_slice(&["-f", "ffmetadata", &metadata_filename]);

        cmd("ffmpeg", ffmpeg_args).run()?;
        Ok(())
    }
    fn remux(&self) -> Result<()> {
        {
            let progress = self.progress.load();
            if progress < Progress::Downloaded {
                warn!("tried to remux {} before it was downloaded, ignoring", self.id.to_string());
                return Ok(());
            } else if progress >= Progress::Remuxed {
                warn!("tried to remux {} in duplicate, ignoring", self.id.to_string());
                return Ok(());
            }
        }
        self.dump_metadata()?;
        info!("remuxing {}", self.id.to_string());
        let output_directory = Path::new("remuxed");
        match std::fs::create_dir(output_directory) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            x => x,
        }?;
        let input_filename = format!("file:{}", self.download_filename().try_into_string()?);
        let metadata_filename = format!("file:{}", &self.filename_with_extension("ffmetadata").try_into_string()?);
        let input_acodec: String = {
            static RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
                regex::Regex::new(r#"(?m)^codec_name=(.+)$(?:.|\n)+?^codec_type=audio$"#).unwrap()
            });
            let buf = cmd("ffprobe", &["-loglevel", "level+warning", "-hide_banner", "-show_streams", &input_filename])
                .stdout_capture()
                .read()?;
            RE.captures(&buf)
                .map(|cs| cs[1].to_owned())
                .with_context(|| format!("Cannot probe codec for {:?}", &self.id.to_string()))?
        };
        let (output_ext, output_acodec) = match &*input_acodec {
            "aac" => ("m4a", "copy"),
            x @ "flac" => (x, "copy"),
            x @ "mp3" => (x, "copy"),
            "vorbis" => ("ogg", "copy"),
            "opus" => ("mkv", "copy"),
            _ => ("flac", "flac"),
        };

        let output_filename =
            format!("file:{}", &output_directory.join(self.filename_with_extension(output_ext)).try_into_string()?);
        if input_filename == output_filename {
            return Ok(());
        }

        let ffmpeg_args = &[
            "-y",
            "-nostats",
            "-nostdin",
            "-loglevel",
            "level+warning",
            "-hide_banner",
            "-i",
            &input_filename,
            "-i",
            &metadata_filename,
            "-map_metadata",
            "1",
            "-vn",
            "-acodec",
            output_acodec,
            &output_filename,
        ];

        cmd("ffmpeg", ffmpeg_args).run()?;
        self.write_progress(Progress::Remuxed)?;
        Ok(())
    }
    fn recode(&self) -> Result<()> {
        {
            let progress = self.progress.load();
            if progress < Progress::Remuxed {
                warn!("tried to recode {} before it was remuxed, ignoring", self.id.to_string());
                return Ok(());
            } else if progress >= Progress::Recoded {
                warn!("tried to recode {} in duplicate, ignoring", self.id.to_string());
                return Ok(());
            }
        }
        info!("recoding {}", self.id.to_string());
        let input_filename: PathBuf =
            glob::glob(Path::new("remuxed").join(format!("{}.*", self.id.to_string())).as_path().try_to_str()?)?
                .filter_map(Result::ok)
                .next()
                .with_context(|| format!("remux lost for {}", self.id.to_string()))?;
        let output_filename: PathBuf = {
            let tmp: String = self.id.to_string();
            let (head, tail) = tmp.split_at(tmp.len() - 8);
            Path::new("recoded").join(head).join(format!("{}.mp3", tail))
        };
        if input_filename == output_filename {
            return Ok(());
        }

        let ffmpeg_args_stats = &[
            "-y",
            "-nostats",
            "-nostdin",
            "-hide_banner",
            "-i",
            input_filename.as_path().try_to_str()?,
            "-af",
            "silenceremove=start_periods=1:start_duration=1:start_threshold=0.02:stop_periods=1:stop_duration=1:stop_threshold=0.02,loudnorm=print_format=json,dual_mono=true",
            "-f",
            "null",
            "/dev/null",
        ];

        static LOUDNORM_STATS_RE: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| regex::Regex::new(r#"(?m)^\s*\{\s*$\p{any}+?^\s*\}\s*$"#).unwrap());

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LoudnormStatsRaw<'a> {
            input_i: &'a str,
            input_tp: &'a str,
            input_lra: &'a str,
            input_thresh: &'a str,
            output_i: &'a str,
            output_tp: &'a str,
            output_lra: &'a str,
            output_thresh: &'a str,
            normalization_type: &'a str,
            target_offset: &'a str,
        }
        struct LoudnormStatsChecked<'a> {
            #[allow(dead_code)]
            input_i: f64,
            #[allow(dead_code)]
            input_tp: f64,
            #[allow(dead_code)]
            input_lra: f64,
            #[allow(dead_code)]
            input_thresh: f64,
            #[allow(dead_code)]
            output_i: f64,
            #[allow(dead_code)]
            output_tp: f64,
            #[allow(dead_code)]
            output_lra: f64,
            #[allow(dead_code)]
            output_thresh: f64,
            #[allow(dead_code)]
            normalization_type: &'a str,
            #[allow(dead_code)]
            target_offset: f64,
        }
        impl<'a> LoudnormStatsRaw<'a> {
            fn checked(self) -> Result<Self> {
                let LoudnormStatsRaw { ref input_i, ref input_tp, ref input_lra, ref input_thresh, ref output_i, ref output_lra, ref output_tp, ref output_thresh, ref normalization_type, ref target_offset } = self;
                let input_i = input_i.parse()?;
                let input_tp = input_tp.parse()?;
                let input_lra = input_lra.parse()?;
                let input_thresh = input_thresh.parse()?;
                let output_i = output_i.parse()?;
                let output_lra = output_lra.parse()?;
                let output_tp = output_tp.parse()?;
                let output_thresh = output_thresh.parse()?;
                let target_offset = target_offset.parse()?;
                let _ = LoudnormStatsChecked { input_i, input_tp, input_lra, input_thresh, output_i, output_lra, output_tp, output_thresh, normalization_type, target_offset };
                Ok(self)
            }
            fn into_audiofilter(self) -> String {
                format!(
                    "silenceremove=start_periods=1:start_duration=1:start_threshold=0.02:stop_periods=1:stop_duration=1:stop_threshold=0.02,loudnorm=linear=true:measured_I={}:measured_LRA={}:measured_tp={}:measured_thresh={}:dual_mono=true",
                    self.input_i, self.input_lra, self.input_tp, self.input_thresh
                )
            }
        }

        let ffmpeg_args = {
            let buf = cmd("ffmpeg", ffmpeg_args_stats).stderr_to_stdout().read()?;
            &[
                "-y",
                "-nostats",
                "-nostdin",
                "-hide_banner",
                "-i",
                input_filename.as_path().try_to_str()?,
                //"-af",
                //"compand=attacks=.3|.3:decays=1|1:points=-90/-60|-60/-40|-40/-30|-20/-20:soft-knee=6:gain=0:volume=-90:delay=1",
                "-af",
                &*serde_json::from_str::<LoudnormStatsRaw>(LOUDNORM_STATS_RE.find(&buf).context("loudnorm stats failed")?.as_str())?.checked()?.into_audiofilter(),
                //"-af",
                //"silenceremove=start_periods=1:start_duration=1:start_threshold=0.02:stop_periods=1:stop_duration=1:stop_threshold=0.02,loudnorm=i=-16:lra=8:tp=0:dual_mono=true",
                "-ar",
                "48000",
                "-c",
                "libmp3lame",
                "-q:a",
                "5",
                &output_filename.as_path().try_to_str()?,
            ]
        };

        cmd("ffmpeg", ffmpeg_args).run()?;
        self.write_progress(Progress::Recoded)?;
        Ok(())
    }
    pub(crate) fn postprocess(&self) -> Result<()> {
        if self.progress.load() < Progress::Remuxed {
            self.remux()?;
        }
        self.recode()?;
        Ok(())
    }
}

#[derive(Debug, Derivative, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[derivative(/*PartialEq, Eq, PartialOrd, Ord*/)]
pub(crate) struct YoutubePlaylist {
    pub(crate) entries: Vec<Option<TargetItem>>,
    title: String,
    id: String,
    webpage_url: String,
    #[serde(flatten)]
    uploader: Option<UploaderDetails>,
    #[serde(flatten)]
    discriminant: DiscriminantDetails,
}
impl YoutubePlaylist {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        self.entries.iter().flatten().map(|item| item.expiry()).flatten().max()
    }
    pub(crate) fn progress(&self) -> Option<Progress> {
        self.entries.par_iter().flatten().map(|t| t.progress.load()).min()
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
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.entries.iter().map(YoutubePlaylist::len).sum()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.iter().all(YoutubePlaylist::is_empty)
    }
    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        self.entries.iter().map(YoutubePlaylist::expiry).flatten().max()
    }
    pub(crate) fn progress(&self) -> Option<Progress> {
        self.entries.par_iter().map(YoutubePlaylist::progress).flatten().min()
    }
    fn singleton_from_playlist(playlist: YoutubePlaylist) -> Self {
        YoutubePlaylists { entries: vec![playlist], ..Default::default() }
    }
}
