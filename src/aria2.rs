use anyhow::{bail, ensure, Context, Result};
use derivative::*;
use duct::cmd;
use log::info;
use rand::{self, thread_rng};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::ytdl;

static ARIA2_TOKEN: once_cell::sync::Lazy<String> = once_cell::sync::Lazy::new(|| {
    use rand::distributions::Distribution;
    "token:".chars().chain(rand::distributions::Alphanumeric.sample_iter(thread_rng()).take(33)).collect()
});
static ARIA2_URL: &str = "http://localhost:6800/jsonrpc";
static RPC_CLIENT: once_cell::sync::Lazy<reqwest::blocking::Client> = once_cell::sync::Lazy::new(Default::default);

pub fn start(use_cookies: bool) -> Result<()> {
    let aria2_exe = "aria2c";
    let temp1 = format!("--rpc-secret={}", ARIA2_TOKEN.splitn(2, ':').last().unwrap());
    let temp2 = format!("--stop-with-process={}", std::process::id());
    //let temp3;
    let mut aria2_args = vec![
        // "--show-console-readout=false", // "--quiet",
        "--disable-ipv6",
        "--force-save=true",
        "--save-session=aria2-session.txt",
        "--always-resume=false",
        "--allow-overwrite=false",
        "--http-accept-gzip=true",
        "--max-connection-per-server=8",
        "--min-split-size=1M",
        "--split=4", //"--split=100",
        "--auto-file-renaming=false",
        "--enable-rpc",
        &*temp1,
        &*temp2,
    ];
    if use_cookies {
        aria2_args.push("--load-cookies=cookies.txt");
    }
    /*if let Some(interface) = interface {
        temp3 = format!("--interface={}", interface);
        aria2_args.push(&*temp3);
    }*/
    info!("starting aria2");

    if cfg!(windows) {
        cmd("aria2c", aria2_args).unchecked().start()?;
    } else {
        match cmd!("systemctl", "cat", "youtube-dl-wrapper-aria2c").unchecked().run()?.status.code() {
            Some(1) => {
                let mut systemd_args = vec![
                    "--user",
                    "--service-type=exec",
                    "--property=Restart=on-failure",
                    "--same-dir",
                    concat!("--unit=", env!("CARGO_PKG_NAME"), "-aria2c"),
                ];
                systemd_args.push(aria2_exe);
                systemd_args.extend(aria2_args);
                cmd("systemd-run", systemd_args).run()?;
            }
            Some(0) => {}
            _ => bail!("systemctl cat failed"),
        }
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
    Ok(())
}

pub trait JsonRpcSend: Serialize {
    type Out: serde::de::DeserializeOwned;
    fn validate(&self, _response: &Self::Out) -> Result<()> { Ok(()) }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcEnvelope<T> {
    jsonrpc: String,
    id: Option<()>,
    #[serde(flatten)]
    pub contents: T,
}
impl<U: serde::de::DeserializeOwned, T: JsonRpcSend<Out = U>> JsonRpcEnvelope<T> {
    fn new(contents: T) -> Self {
        JsonRpcEnvelope { jsonrpc: "2.0".to_owned(), id: None, contents }
    }
    #[inline]
    pub fn send(&self) -> Result<JsonRpcEnvelope<U>> {
        self.send_with(&RPC_CLIENT)
    }
    pub fn send_with(&self, client: &reqwest::blocking::Client) -> Result<JsonRpcEnvelope<U>> {
        let resp = client.post(ARIA2_URL).json(self).send()?;
        let ret: JsonRpcEnvelope<U> = match resp.error_for_status_ref() {
            Ok(_) => resp.json()?,
            Err(e) => Err(e).with_context(|| {
                format!(
                    "sent {}, got {}",
                    serde_json::to_string_pretty(&self).unwrap_or_else(|e| format!("<deser error: {:?}>", e)),
                    resp.text().unwrap()
                )
            })?,
        };
        self.contents.validate(&ret.contents)?;
        Ok(ret)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Gid(pub(crate) u64);
impl From<ytdl::ItemId> for Gid {
    fn from(x: ytdl::ItemId) -> Self {
        Gid(x.0)
    }
}
impl<'de> Deserialize<'de> for Gid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(Gid(u64::from_ne_bytes(hex::deserialize(deserializer)?)))
    }
}
impl Serialize for Gid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        hex::serialize(self.0.to_ne_bytes(), serializer)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct AddUriOptions<'a, 'b> {
    pub dir: PathBuf,
    pub gid: Gid,
    pub header: Vec<&'a str>,
    pub out: &'b str,
}

#[derive(Debug, Serialize)]
pub struct AddUriParams<'a, 'b, 'c> {
    method: &'static str,
    params: (&'static str, (&'a str,), AddUriOptions<'b, 'c>),
}
impl<'a, 'b, 'c> AddUriParams<'a, 'b, 'c> {
    pub fn new(url: &'a str, options: AddUriOptions<'b, 'c>) -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(AddUriParams { method: "aria2.addUri", params: (&*ARIA2_TOKEN, (url,), options) })
    }
}
impl<'a, 'b, 'c> JsonRpcSend for AddUriParams<'a, 'b, 'c> {
    type Out = AddUriResult;
    fn validate(&self, response: &Self::Out) -> Result<()> {
        ensure!(response.result == self.params.2.gid, "aria2.addUri: bad response: {:#?}", response);
        Ok(())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddUriResult {
    result: Gid,
}

#[derive(Debug, Serialize)]
pub struct ChangeUriParams<'b, 'c> {
    method: &'static str,
    params: (&'static str, Gid, usize, (&'b str,), (&'c str,)),
}
impl<'b, 'c> ChangeUriParams<'b, 'c> {
    pub fn new(gid: Gid, old_url: &'b str, new_url: &'c str) -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(ChangeUriParams {
            method: "aria2.changeUri",
            params: (&*ARIA2_TOKEN, gid, 1, (old_url,), (new_url,)),
        })
    }
}
impl<'b, 'c> JsonRpcSend for ChangeUriParams<'b, 'c> {
    type Out = ChangeUriResult;
    fn validate(&self, response: &Self::Out) -> Result<()> {
        ensure!(response.result == (1, 1), "{}: bad response: {:#?}", self.method, response);
        Ok(())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeUriResult {
    result: (usize, usize),
}

#[derive(Debug, Serialize)]
pub struct RemoveParams {
    method: &'static str,
    params: (&'static str, Gid),
}
impl RemoveParams {
    #[allow(dead_code)]
    pub fn new(gid: Gid) -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(RemoveParams { method: "aria2.remove", params: (&*ARIA2_TOKEN, gid) })
    }
}
impl JsonRpcSend for RemoveParams {
    type Out = RemoveResult;
    fn validate(&self, response: &Self::Out) -> Result<()> {
        ensure!(response.result == self.params.1, "{}: bad response: {:#?}", self.method, response);
        Ok(())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveResult {
    result: Gid,
}

#[derive(Debug, Serialize)]
pub struct RemoveResultParams {
    method: &'static str,
    params: (&'static str, Gid),
}
impl RemoveResultParams {
    pub fn new(gid: Gid) -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(RemoveResultParams { method: "aria2.removeDownloadResult", params: (&*ARIA2_TOKEN, gid) })
    }
}
impl JsonRpcSend for RemoveResultParams {
    type Out = RemoveResultResult;
    fn validate(&self, response: &Self::Out) -> Result<()> {
        ensure!(response.result == "OK", "{}: bad response: {:#?}", self.method, response);
        Ok(())
    }
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveResultResult {
    result: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum DownloadStatus {
    Waiting,
    Paused,
    Removed,
    Active,
    Error,
    Complete,
}
#[derive(Debug, Deserialize, Derivative)]
#[derivative(PartialEq, PartialOrd, Eq, Ord)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Download {
    pub gid: Gid,
    #[derivative(PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]
    pub status: DownloadStatus,
    #[derivative(PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]
    #[serde(default)]
    pub error_code: String,
    #[derivative(PartialEq="ignore", PartialOrd="ignore", Ord="ignore")]
    #[serde(default)]
    pub error_message: String,
}
impl Download {
    const KEYS: &'static [&'static str; 4] = &["gid", "status", "errorCode", "errorMessage"];
}

#[derive(Debug, Serialize)]
pub(crate) struct TellStatusParams {
    method: &'static str,
    params: (&'static str, Gid, &'static [&'static str]),
}
impl TellStatusParams {
    #[allow(dead_code)]
    pub(crate) fn new(gid: Gid) -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(TellStatusParams {
            method: "aria2.tellStatus",
            params: (&*ARIA2_TOKEN, gid, Download::KEYS),
        })
    }
}
impl JsonRpcSend for TellStatusParams {
    type Out = TellStatusResult;
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TellStatusResult {
    pub result: Download,
}

#[derive(Debug, Serialize)]
pub struct TellActiveParams {
    method: &'static str,
    params: (&'static str, &'static [&'static str]),
}
impl TellActiveParams {
    pub fn new() -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(TellActiveParams { method: "aria2.tellActive", params: (&*ARIA2_TOKEN, Download::KEYS) })
    }
}
impl JsonRpcSend for TellActiveParams {
    type Out = TellActiveResult;
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TellActiveResult {
    pub result: Vec<Download>,
}

#[derive(Debug, Serialize)]
pub struct TellStoppedParams {
    method: &'static str,
    params: (&'static str, usize, usize, &'static [&'static str]),
}
impl TellStoppedParams {
    pub fn new() -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(TellStoppedParams {
            method: "aria2.tellStopped",
            params: (&*ARIA2_TOKEN, 0, 100_000, Download::KEYS),
        })
    }
}
impl JsonRpcSend for TellStoppedParams {
    type Out = TellStoppedResult;
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TellStoppedResult {
    pub result: Vec<Download>,
}

#[derive(Debug, Serialize)]
pub struct TellWaitingParams {
    method: &'static str,
    params: (&'static str, usize, usize, &'static [&'static str]),
}
impl TellWaitingParams {
    pub fn new() -> JsonRpcEnvelope<Self> {
        JsonRpcEnvelope::new(TellWaitingParams {
            method: "aria2.tellWaiting",
            params: (&*ARIA2_TOKEN, 0, 100_000, Download::KEYS),
        })
    }
}
impl JsonRpcSend for TellWaitingParams {
    type Out = TellWaitingResult;
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TellWaitingResult {
    pub result: Vec<Download>,
}
