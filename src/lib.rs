#![warn(clippy::all)]
#![deny(clippy::todo)]
#[allow(unused_imports)]
use anyhow::{anyhow, bail, ensure, Context, Result};
use extension_trait::extension_trait;
#[allow(unused_imports)]
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

mod aria2;
pub mod ytdl;

#[extension_trait]
impl<T, U> OptionExtPair<T, U> for Option<T> {
    fn zip(self, optb: Option<U>) -> Option<(T, U)> {
        Some((self?, optb?))
    }
    fn rzip(self, optb: Option<U>) -> Option<(U, T)> {
        Some((optb?, self?))
    }
}
#[extension_trait]
impl<T, E> ResultExt<T, E> for std::result::Result<T, E> {
    fn ok_but(self, f: impl FnOnce(E) -> ()) -> Option<T> {
        match self {
            Ok(x) => Some(x),
            Err(x) => {
                f(x);
                None
            }
        }
    }
}

#[extension_trait]
impl<T> TryRecvErrorOptionExt<T> for Result<T, crossbeam_channel::TryRecvError> {
    fn empty_ok(self) -> Result<Option<T>, ()> {
        match self {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Err(()),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
        }
    }
}

#[extension_trait]
impl<T> RecvTimeoutErrorOptionExt<T> for Result<T, crossbeam_channel::RecvTimeoutError> {
    fn timeout_ok(self) -> Result<Option<T>, ()> {
        match self {
            Ok(x) => Ok(Some(x)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(()),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => Ok(None),
        }
    }
}

#[extension_trait]
impl<T> SendTimeoutErrorOptionExt<T> for Result<(), crossbeam_channel::SendTimeoutError<T>> {
    fn timeout_ok(self) -> Result<(), ()> {
        match self {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => Err(()),
            Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => Ok(()),
        }
    }
}

macro_rules! try_get {
    ($map:ident, $key:expr) => {
        $map.get(&$key).with_context(|| anyhow!("lost track of {:?}", $key))
    };
    (mut $map:ident, $key:expr) => {
        $map.get_mut(&$key).with_context(|| anyhow!("lost track of {:?}", $key))
    };
}

macro_rules! try_remove {
    ($map:ident, $key:expr) => {
        $map.remove(&$key).with_context(|| anyhow!("lost track of {:?}", $key))
    };
}
macro_rules! borrow {
    ($x:ident) => {
        let $x = &$x;
    };
    (mut $x:ident) => {
        let $x = &mut $x;
    };
}
macro_rules! clone {
    ($x:ident) => {
        let $x = $x.clone();
    };
    (mut $x:ident) => {
        let mut $x = $x.clone();
    };
}
macro_rules! moove {
    ($x:ident) => {
        let $x = $x;
    };
    (mut $x:ident) => {
        let mut $x = $x;
    };
}
macro_rules! try_ok {
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(_) => return Ok(()),
        }
    };
}

#[allow(clippy::block_in_if_condition_stmt)]
fn download_things(
    urls: Vec<ytdl::StartingPoint>, ignores: BTreeSet<ytdl::ItemId>, use_cookies: bool, postprocess: bool,
    limit: Option<usize>,
) -> Result<()> {
    static STOP_REQUESTED: crossbeam_utils::atomic::AtomicCell<bool> = crossbeam_utils::atomic::AtomicCell::new(false);
    ctrlc::set_handler(|| {
        STOP_REQUESTED.store(true);
    })?;
    aria2::start(use_cookies)?;

    let item_index: dashmap::DashMap<ytdl::ItemId, ytdl::TargetItem> = dashmap::DashMap::new();

    let postprocess_stop_requested = crossbeam_utils::atomic::AtomicCell::new(false);
    let poll_tick = crossbeam_channel::tick(Duration::from_secs(5));
    let enqueue_tick = crossbeam_channel::tick(Duration::from_millis(500));
    let (enqueue_complete_tx, enqueue_complete_rx) = crossbeam_channel::bounded::<()>(0);
    let (poll_waiting_tx, poll_waiting_rx) = crossbeam_channel::bounded::<_>(1);
    let (poll_stopped_tx, poll_stopped_rx) = crossbeam_channel::bounded::<_>(1);
    let (pollstats_tx, pollstats_rx) = crossbeam_channel::bounded::<_>(1);

    let (enqueue_tx, enqueue_rx) = crossbeam_channel::bounded::<ytdl::ItemId>(300);
    let (postprocess_tx, postprocess_rx) = crossbeam_channel::unbounded::<ytdl::ItemId>();
    let (amend_tx, amend_rx) = crossbeam_channel::unbounded::<ytdl::ItemId>();

    crossbeam_utils::thread::scope(|scope| {
        let mut rret: Vec<crossbeam_utils::thread::ScopedJoinHandle<Result<()>>> = vec![];

        rret.push(scope.builder().name("url items collector".to_owned()).spawn({
            borrow!(item_index);
            clone!(postprocess_tx);
            moove!(enqueue_tx);
            moove!(ignores);
            move |_scope| {
                let mut spcount = limit.unwrap_or(std::usize::MAX);
                for mut sp in urls {
                    if STOP_REQUESTED.load() || spcount == 0 {
                        return Ok(());
                    }
                    let items: Vec<ytdl::TargetItem> = sp
                        .read(false, use_cookies, postprocess)
                        .ok_but(|e| warn!("Failed to read {:?}, abandoning: {:?}", sp.id, e))
                        .unwrap_or_default();
                    let itemcount = crossbeam_utils::atomic::AtomicCell::<usize>::new(0);
                    try_ok!(items.into_par_iter().try_for_each(|item| -> Result<(), ()> {
                        if let dashmap::mapref::entry::Entry::Vacant(ve) = item_index.entry(item.id) {
                            let r = ve.insert(item).downgrade();
                            if ignores.contains(r.key()) {
                                info!("ignoring {}", r.key().to_string());
                            } else if match r.value().progress() {
                                ytdl::Progress::Unknown | ytdl::Progress::Interrupted => Some(&enqueue_tx),
                                ytdl::Progress::Queued => Some(&enqueue_tx),
                                ytdl::Progress::Downloaded => Some(&postprocess_tx),
                                ytdl::Progress::Remuxed => Some(&postprocess_tx),
                                ytdl::Progress::Recoded => None,
                            }.map(|c| c.send(*r.key()).map_err(drop)).transpose()?.is_some() {
                                itemcount.fetch_add(1);
                            }
                        }
                        Ok(())
                    }));
                    let itemcount = itemcount.into_inner();
                    if itemcount > 0 {
                        spcount -= 1;
                    }
                    info!("rfe'd {} items from {}", itemcount, sp.id);
                }
                Ok(())
            }
        })?);

        rret.push(scope.builder().name("aria2 poller".to_owned()).spawn({
            moove!(pollstats_tx);
            moove!(poll_waiting_tx);
            moove!(poll_stopped_tx);
            move |_scope| {
                let mut enqueueing_complete = false;
                for _ in poll_tick {
                    let (w, a, s) = ytdl::TargetItem::poll_all_downloads()?;
                    let t = (w.len(), a.len(), s.len());
                    try_ok!(pollstats_tx.send_timeout(t, Duration::from_secs(1)).timeout_ok());
                    try_ok!(pollstats_tx.send_timeout(t, Duration::from_secs(1)).timeout_ok());
                    try_ok!(poll_waiting_tx.send(w));
                    try_ok!(poll_stopped_tx.send(s));
                    enqueueing_complete = enqueueing_complete
                        || enqueue_complete_rx.try_recv() == Err(crossbeam_channel::TryRecvError::Disconnected);
                    if enqueueing_complete && (t.0 | t.1 | t.2) == 0 {
                        return Ok(());
                    }
                }
                Ok(())
            }
        })?);

        for i in 0..4 {
            borrow!(item_index);
            borrow!(postprocess_stop_requested);
            clone!(postprocess_rx);
            rret.push(scope.builder().name(format!("postproc #{}", i + 1)).spawn(
                move |_scope| -> Result<()> {
                    if postprocess {
                        for itemid in postprocess_rx {
                            if postprocess_stop_requested.load() {
                                return Ok(());
                            }
                            if let Some((_itemid, item)) = try_remove!(item_index, itemid).ok_but(|e| error!("{:?}", e))
                            {
                                item.postprocess().map_err(|e| {
                                    postprocess_stop_requested.store(true);
                                    e
                                })?;
                            }
                        }
                    } else {
                        for itemid in postprocess_rx {
                            if postprocess_stop_requested.load() {
                                return Ok(());
                            }
                            if let Some((_itemid, item)) = try_remove!(item_index, itemid).ok_but(|e| error!("{:?}", e))
                            {
                                item.dump_metadata().map_err(|e| {
                                    postprocess_stop_requested.store(true);
                                    e
                                })?;
                            }
                        }
                    }
                    Ok(())
                },
            )?);
        }
        drop(postprocess_rx);

        rret.push(scope.builder().name("item modifications writebacker".to_owned()).spawn({
            borrow!(item_index);
            moove!(amend_rx);
            move |_scope| {
                for itemid in amend_rx {
                    if let Some(item) = try_get!(item_index, itemid).ok_but(|e| error!("{:?}", e)) {
                        item.write()?;
                    }
                }
                Ok(())
            }
        })?);

        rret.push(scope.builder().name("item download enqueuer".to_owned()).spawn({
            borrow!(item_index);
            clone!(amend_tx);
            clone!(postprocess_tx);
            clone!(pollstats_rx);
            moove!(enqueue_rx);
            move |_scope| {
                let _enqueue_complete_tx = enqueue_complete_tx;
                let client = reqwest::blocking::Client::new();
                let mut stats = try_ok!(pollstats_rx.recv());
                for itemid in enqueue_rx {
                    let _ = try_ok!(enqueue_tick.recv());
                    if let Some(newstats) = try_ok!(pollstats_rx.try_recv().empty_ok()) {
                        stats = newstats;
                    }
                    while stats.0 + stats.1 >= 8 {
                        stats = try_ok!(pollstats_rx.recv());
                    }
                    match try_get!(item_index, itemid).map(|item| item.queue_download(&client, use_cookies)) {
                        Ok(Err(e)) => {
                            warn!("failed to enqueue {}, abandoning it: {:?}", itemid.to_string(), e);
                            for _ in 0..5 {
                                try_ok!(enqueue_tick.recv());
                            }
                        }
                        Err(e) => error!("{:?}", e),
                        Ok(Ok(ytdl::AlreadyDownloaded)) => try_ok!(postprocess_tx.send(itemid)),
                        Ok(Ok(ytdl::Queued)) => stats.0 += 1,
                        Ok(Ok(ytdl::QueuedButMustAmend)) => {
                            stats.0 += 1;
                            try_ok!(amend_tx.send(itemid))
                        }
                    }
                }
                Ok(())
            }
        })?);

        rret.push(scope.builder().name("aria2 pending download refreshener".to_owned()).spawn({
            borrow!(item_index);
            moove!(pollstats_rx);
            moove!(poll_waiting_rx);
            move |_scope| {
                for (waiting, (_nw, na, _ns)) in poll_waiting_rx.into_iter().zip(pollstats_rx) {
                    waiting.into_iter().take(na * 4).try_for_each(|dl: aria2::Download| -> Result<()> {
                        if let Some(item) =
                            try_get!(item_index, ytdl::ItemId::from(dl.gid)).ok_but(|e| error!("{:?}", e))
                        {
                            if item.requeue_if_needed(use_cookies)? {
                                try_ok!(amend_tx.send(dl.gid.into()));
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }
        })?);

        rret.push(scope.builder().name("aria2 finished download reactor".to_owned()).spawn({
            borrow!(item_index);
            moove!(poll_stopped_rx);
            moove!(poll_stopped_rx);
            moove!(postprocess_tx);
            move |_scope| {
                let client = reqwest::blocking::Client::new();
                for stopped in poll_stopped_rx {
                    for dl in stopped {
                        match &dl.status {
                            aria2::DownloadStatus::Complete => {
                                if let Some(item) = try_get!(item_index, ytdl::ItemId::from(dl.gid))
                                    .ok_but(|e| error!("download succeeded but item lost: {:?}", e))
                                {
                                    item.write_progress(ytdl::Progress::Downloaded)?;
                                    aria2::RemoveResultParams::new(dl.gid).send_with(&client)?;
                                    try_ok!(postprocess_tx.send(dl.gid.into()));
                                }
                            }
                            aria2::DownloadStatus::Error => {
                                error!(
                                    "download of {} failed, code {:?}: {:?}",
                                    ytdl::ItemId::from(dl.gid).to_string(),
                                    dl.error_code,
                                    dl.error_message
                                );
                                if let Some(item) =
                                    try_get!(item_index, ytdl::ItemId::from(dl.gid)).ok_but(|e| error!("{:?}", e))
                                {
                                    item.write_progress(ytdl::Progress::Interrupted)?;
                                }
                                aria2::RemoveResultParams::new(dl.gid).send()?;
                            }
                            aria2::DownloadStatus::Removed => {
                                warn!(
                                    "someone pulled {} out from under us; continuing",
                                    ytdl::ItemId::from(dl.gid).to_string()
                                );
                                aria2::RemoveResultParams::new(dl.gid).send_with(&client)?;
                            }
                            aria2::DownloadStatus::Waiting
                            | aria2::DownloadStatus::Paused
                            | aria2::DownloadStatus::Active => error!(
                                "tellStopped gave us item {:?}/{} which was actually {:?}",
                                dl.gid,
                                ytdl::ItemId::from(dl.gid).to_string(),
                                dl.status
                            ),
                        }
                    }
                }
                Ok(())
            }
        })?);

        // every sender should be U.o.M.V. here:
        /*
        drop(enqueue_complete_tx);
        drop(enqueue_tx); drop(postprocess_tx); drop(amend_tx);
        drop(poll_waiting_tx); drop(poll_stopped_tx); drop(pollstats_tx); */
        rret.into_iter().filter_map(|h| h.join().ok()).collect::<Result<Vec<_>>>()
    })
    .unwrap()?;

    Ok(())
}

fn read_files<'o, 'i1: 'o, 'i2: 'o>(
    files: impl IntoIterator<Item = &'i1 PathBuf>, buffer: &'i2 mut String,
) -> Result<(Vec<std::ops::Range<usize>>, &'o str)> {
    let mut i: usize = 0;
    let mut file_offsets = vec![];
    for file in files {
        let start = i;
        let len = std::fs::File::open(file)?.read_to_string(buffer)?;
        i = i.checked_add(len).context("usize overflow from the url-file buffer")?;
        file_offsets.push(start..len);
    }
    Ok((file_offsets, buffer.as_str()))
}

fn parse_input_files<'a>(
    tuple: (Vec<std::ops::Range<usize>>, &'a str),
) -> impl ParallelIterator<Item = Result<ytdl::StartingPoint<'a>>> {
    let (file_offsets, buffer) = tuple;
    file_offsets
        .into_par_iter()
        .flat_map(move |range: std::ops::Range<usize>| {
            (&buffer[range])
                .par_lines()
                .filter_map(|s: &str| s.splitn(2, |c: char| "#;".contains(c)).next())
                .filter_map(|s: &str| s.split_ascii_whitespace().last())
                .filter(|s: &&str| !s.is_empty())
        })
        .map(ytdl::StartingPoint::new)
}

fn parse_ignore_files<'a>(
    tuple: (Vec<std::ops::Range<usize>>, &'a str),
) -> impl ParallelIterator<Item = Result<ytdl::ItemId>> + 'a {
    let (file_offsets, buffer) = tuple;
    file_offsets
        .into_par_iter()
        .flat_map(move |range: std::ops::Range<usize>| {
            (&buffer[range])
                .par_lines()
                .filter_map(|s| s.splitn(2, |c: char| "#;".contains(c)).next())
                .filter_map(|s| s.splitn(2, '.').next())
                .filter(|s: &&str| !s.is_empty())
        })
        .map(|s| str::parse(s))
}

#[derive(StructOpt, Debug)]
#[structopt()]
struct Opt {
    /// enable postprocessing
    #[structopt(short, long)]
    postprocess: bool,
    /// only use the first LIMIT metadata URLs
    #[structopt(short, long)]
    limit: Option<usize>,
    /// use cookiejar
    #[structopt(short = "c", long)]
    use_cookies: bool,
    /// change to directory DIR
    #[structopt(short = "C", long, name = "DIR", parse(from_os_str))]
    directory: Option<PathBuf>,
    /// list of IDs to ignore
    #[structopt(short, long, name = "FILE", parse(from_os_str))]
    ignore_file: Option<PathBuf>,
    /// lists of metadata URLs
    #[structopt(name = "FILE", required(true), parse(from_os_str))]
    files: Vec<PathBuf>,
}

pub fn main() -> Result<()> {
    //rayon::ThreadPoolBuilder::new().num_threads(2).build_global()?;
    env_logger::Builder::from_default_env().filter(None, LevelFilter::Info).init();
    let opt: Opt = {
        let mut opt = Opt::from_args();
        opt.files.par_iter_mut().try_for_each(|p| -> Result<()> {
            *p = p.canonicalize()?;
            Ok(())
        })?;
        opt.directory.take().map(std::env::set_current_dir).transpose()?;
        opt
    };

    let mut file_contents_buffer = Default::default();
    let mut ignorefile_contents_buffer = Default::default();
    let urls = parse_input_files(read_files(&opt.files, &mut file_contents_buffer)?).collect::<Result<_>>()?;
    let ignores =
        parse_ignore_files(read_files(&opt.ignore_file, &mut ignorefile_contents_buffer)?).collect::<Result<_>>()?;
    download_things(urls, ignores, opt.use_cookies, opt.postprocess, opt.limit)?;

    Ok(())
}
