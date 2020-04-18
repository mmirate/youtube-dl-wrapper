#![warn(clippy::all)]
#![deny(clippy::todo)]
#[allow(unused_imports)]
use anyhow::{anyhow, bail, ensure, Context, Result};
use duct::cmd;
use extension_trait::extension_trait;
#[allow(unused_imports)]
use log::{debug, info, warn, error, LevelFilter};
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use structopt::StructOpt;

mod aria2;
pub mod ytdl;
use ytdl::PathExt;

#[extension_trait]
impl<T, U> OptionExt<T, U> for Option<T> {
    fn zip(self, optb: Option<U>) -> Option<(T, U)> {
        Some((self?, optb?))
    }
    fn rzip(self, optb: Option<U>) -> Option<(U, T)> {
        Some((optb?, self?))
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

#[allow(clippy::block_in_if_condition_stmt)]
fn download_things<'a, 'b>(things: &'a BTreeMap<ytdl::StartingPoint, ytdl::YoutubePlaylists>, use_cookies: bool, interface: Option<&'b str>) -> Result<()> {
    aria2::start(use_cookies, interface)?;

    let rewrites = ytdl::YoutubePlaylists::find_dupes(things.par_iter().map(|(_k, v)| v))?;
    things.par_iter().try_for_each(|(sp, pls)| -> Result<()> {
        if rewrites.contains(sp.id) {
            sp.amend(pls)?;
        }
        Ok(())
    })?;

    info!("dupechecks complete");

    macro_rules! borrow {
        ($x:ident) => {
            let $x = &$x;
        };
    }
    macro_rules! clone {
        ($x:ident) => {
            let $x = $x.clone();
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

    let index: BTreeMap<ytdl::ItemId, (&ytdl::StartingPoint, &ytdl::TargetItem)> = things
        .par_iter()
        .flat_map(move |(sp, pls)| {
            pls.entries
                .par_iter()
                .filter(|pl| !pl.dupe.load())
                .flat_map(move |pl| pl.entries.par_iter().flatten())
                .filter(|t| !t.dupe.load())
                .map(move |t| (t.id, (sp, t)))
        })
        .collect();

    let ready_for_enqueue: std::collections::VecDeque<(&ytdl::StartingPoint, &ytdl::TargetItem)> = index
        .par_iter()
        .map(|(_k, (sp, t))| -> Result<Option<(&ytdl::StartingPoint, &ytdl::TargetItem)>> {
            Ok(match t.read_progress()? {
                ytdl::Progress::Unknown | ytdl::Progress::Queued => Some((sp, t)),
                ytdl::Progress::Interrupted => {
                    match t.queue_download(&reqwest::blocking::Client::new(), use_cookies)? {
                        ytdl::AlreadyDownloaded => {
                            t.postprocess()?;
                            None
                        }
                        ytdl::Queued => None,
                        ytdl::QueuedButMustAmend => {
                            sp.amend(
                                things
                                    .get(sp)
                                    .with_context(|| anyhow!("lost track of parsed channel for {:?}", &sp.url))?,
                            )?;
                            None
                        }
                    }
                }
                ytdl::Progress::Downloaded => {
                    t.postprocess()?;
                    None
                }
                ytdl::Progress::Postprocessed => None,
            })
        })
        .map(Result::transpose)
        .flatten()
        .collect::<Result<_>>()?;

    info!("data-shuffling complete");

    let poll_tick = crossbeam_channel::tick(std::time::Duration::from_secs(5));
    let enqueue_tick = crossbeam_channel::tick(std::time::Duration::from_secs(1));
    let (enqueue_complete_tx, enqueue_complete_rx) = crossbeam_channel::bounded::<()>(0);
    let (poll_waiting_tx, poll_waiting_rx) = crossbeam_channel::bounded::<_>(1);
    let (poll_stopped_tx, poll_stopped_rx) = crossbeam_channel::bounded::<_>(1);
    let (pollstats1_tx, pollstats1_rx) = crossbeam_channel::bounded::<_>(1);
    let (pollstats2_tx, pollstats2_rx) = crossbeam_channel::bounded::<_>(1);
    //let (rfeq_tx, rfeq_rx) = crossbeam_channel::unbounded::<_>();
    let (postprocess_tx, postprocess_rx) = crossbeam_channel::unbounded::<&ytdl::TargetItem>();
    let (amend_tx, amend_rx) = crossbeam_channel::unbounded::<&ytdl::StartingPoint>();

    crossbeam_utils::thread::scope(|scope| {
        let mut rret: Vec<crossbeam_utils::thread::ScopedJoinHandle<Result<()>>> = vec![];
        rret.push(scope.spawn({clone!(postprocess_tx); move |_scope| {
            let mut enqueueing_complete = false;
            for _ in poll_tick {
                let (w, a, s) = ytdl::TargetItem::poll_all_downloads()?;
                let t = (w.len(), a.len(), s.len());
                try_ok!(|| -> Result<()> {
                    pollstats1_tx.send_timeout(t, std::time::Duration::from_secs(1))?;
                    pollstats2_tx.send_timeout(t, std::time::Duration::from_secs(1))?;
                    poll_waiting_tx.send(w)?;
                    poll_stopped_tx.send(s)?;
                    Ok(())
                }());
                if !enqueueing_complete { if enqueue_complete_rx.try_recv().empty_ok().is_err() {
                    enqueueing_complete = true;
                } } else if t.0 + t.1 + t.2 + postprocess_tx.len() == 0 {
                    return Ok(());
                }
            }
            Ok(())
        }}));
        for _ in 0..2 {
            clone!(postprocess_rx);
            rret.push(scope.spawn(move |_scope| -> Result<()> {
                for item in postprocess_rx {
                    item.postprocess()?
                }
                Ok(())
            }));
        }
        drop(postprocess_rx);
        rret.push(scope.spawn({
            borrow!(things);
            move |_scope| {
                for sp in amend_rx {
                    sp.amend(
                        things.get(sp).with_context(|| anyhow!("lost track of parsed channel for {:?}", &sp.url))?,
                    )?;
                }
                Ok(())
            }
        }));
        rret.push(scope.spawn({
            clone!(amend_tx);
            clone!(postprocess_tx);
            move |_scope| {
                let _enqueue_complete_tx = enqueue_complete_tx;
                let client = reqwest::blocking::Client::new();
                let mut ready_for_enqueue = ready_for_enqueue;
                let mut stats = try_ok!(pollstats1_rx.recv());
                while let Some((sp, item)) = ready_for_enqueue.pop_front() {
                    let _ = enqueue_tick.recv();
                    if let Some(newstats) = try_ok!(pollstats1_rx.try_recv().empty_ok()) { stats = newstats; }
                    while stats.0 + stats.1 >= 8 {
                        stats = try_ok!(pollstats1_rx.recv());
                    }
                    match item.queue_download(&client, use_cookies) {
                        Err(e) => {
                            warn!("failed to enqueue {}, backing off: {}", item.id.to_string(), e);
                            ready_for_enqueue.push_back((sp, item));
                            for _ in 0..5 {
                                try_ok!(enqueue_tick.recv());
                            }
                        }
                        Ok(ytdl::AlreadyDownloaded) => try_ok!(postprocess_tx.send(item)),
                        Ok(ytdl::Queued) => stats.0 += 1,
                        Ok(ytdl::QueuedButMustAmend) => { stats.0 += 1; try_ok!(amend_tx.send(sp)) },
                    }
                }
                Ok(())
            }
        }));
        rret.push(scope.spawn({
            borrow!(index);
            move |_scope| {
                for (waiting, (_nw, na, _ns)) in poll_waiting_rx.into_iter().zip(pollstats2_rx) {
                    waiting.into_iter().take(na * 4).try_for_each(|dl: aria2::Download| -> Result<()> {
                        let &(sp, item) =
                            index.get(&dl.gid.into()).with_context(|| format!("lost track of {:?}", dl.gid))?;
                        if item.requeue_if_needed(use_cookies)? {
                            try_ok!(amend_tx.send(sp));
                        }
                        Ok(())
                    })?;
                }
                Ok(())
            }
        }));
        rret.push(scope.spawn({
            borrow!(index);
            move |_scope| {
                let client = reqwest::blocking::Client::new();
                for stopped in poll_stopped_rx {
                    for dl in stopped {
                        let &(_sp, item) =
                            index.get(&dl.gid.into()).with_context(|| format!("lost track of {:?}", dl.gid))?;
                        match &dl.status {
                            aria2::DownloadStatus::Complete => {
                                item.write_progress(ytdl::Progress::Downloaded)?;
                                try_ok!(postprocess_tx.send(item));
                                aria2::RemoveResultParams::new(dl.gid).send_with(&client)?;
                            }
                            aria2::DownloadStatus::Error => bail!(
                                "download of {} failed, code {:?}: {:?}",
                                item.id.to_string(),
                                dl.error_code,
                                dl.error_message
                            ),
                            aria2::DownloadStatus::Removed => {
                                warn!("someone pulled {} out from under us; continuing", item.id.to_string());
                                aria2::RemoveResultParams::new(dl.gid).send_with(&client)?;
                            }
                            aria2::DownloadStatus::Waiting
                            | aria2::DownloadStatus::Paused
                            | aria2::DownloadStatus::Active => error!(
                                "tellStopped gave us item {:?}/{} which was actually {:?}",
                                dl.gid,
                                item.id.to_string(),
                                dl.status
                            ),
                        }
                    }
                }
                Ok(())
            }
        }));
        // every sender should be U.o.M.V. here:
        // drop(poll_waiting_tx); drop(poll_stopped_tx); drop(pollstats1_tx); drop(pollstats2_tx); drop(postprocess_tx); drop(amend_tx);
        rret.into_iter().filter_map(|h| h.join().ok()).collect::<Result<Vec<_>>>()
    })
    .unwrap()?;

    // Ok(());
    todo!(r#"TODO:
    ctrlc
    scrape and parse in-pipeline with everything else (all dataplumbing via channels, bounded backpressure to avoid stale scrapes, etc.)
    rescrape entire StartingPoint if it's full of stale dl links
    aria2c --interface=tun0
    "#)
}

fn read_input_files<'o, 'i1: 'o, 'i2: 'o>(
    files: impl IntoIterator<Item = &'i1 PathBuf>, buffer: &'i2 mut String,
) -> Result<impl ParallelIterator<Item = Result<ytdl::StartingPoint<'o>>>> {
    let mut i: usize = 0;
    let mut file_offsets = vec![];
    for file in files {
        let start = i;
        let len = std::fs::File::open(file)?.read_to_string(buffer)?;
        i = i.checked_add(len).context("usize overflow from the url-file buffer")?;
        file_offsets.push(start..len);
    }
    let buffer = buffer.as_str(); // ixnay the mut borrow so we can slice it like salami
    Ok(file_offsets
        .into_par_iter()
        .flat_map(move |range: std::ops::Range<usize>| {
            (&buffer[range])
                .par_lines()
                .filter_map(|s: &str| s.splitn(2, |c: char| "#;".contains(c)).next())
                .filter_map(|s: &str| s.split_ascii_whitespace().last())
                .filter(|s: &&str| !s.is_empty())
        })
        .map(ytdl::StartingPoint::new))
}

#[derive(StructOpt, Debug)]
#[structopt()]
struct Opt {
    /// download data via parsed metadata
    #[structopt(short, long)]
    download: bool,
    /// scrape metadata (else, only read from disk cache)
    #[structopt(short, long)]
    scrape: bool,
    /// deserialize scraped metadata
    #[structopt(short, long, required_if("download", "true"))]
    parse: bool,
    /// only use the first LIMIT metadata URLs
    #[structopt(short, long)]
    limit: Option<usize>,
    /// use cookiejar
    #[structopt(short, long)]
    cookies: bool,
    /// change to directory DIR
    #[structopt(short, long)]
    interface: Option<String>,
    /// change to directory DIR
    #[structopt(short = "C", long, name = "DIR", parse(from_os_str))]
    directory: Option<PathBuf>,
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

    let mut file_contents_buffer = String::new();
    let urls =
        read_input_files(&opt.files, &mut file_contents_buffer)?.collect::<Result<Vec<ytdl::StartingPoint>>>()?;
    let (pending_changes_tx, pending_changes_rx) = crossbeam_channel::unbounded::<String>();
    let sedscript_path = std::env::current_dir()?.join("pending_changes.sed");
    let mut pending_changes =
        std::fs::OpenOptions::new().read(false).write(true).create_new(true).open(&sedscript_path).or_else(
            |e| -> Result<_> {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    let mut sed_args: Vec<Result<&str>> =
                        vec![Ok("-i.bak"), Ok("-f"), sedscript_path.as_path().try_to_str(), Ok("--")];
                    sed_args.extend(opt.files.iter().map(PathBuf::as_path).map(ytdl::PathExt::try_to_str));
                    let cmd = cmd("sed", sed_args.into_iter().collect::<Result<Vec<_>>>()?);
                    warn!("sed script already exists, running it inplace via {:?} then deleting it", cmd);
                    cmd.run()?;
                    std::fs::OpenOptions::new()
                        .read(false)
                        .write(true)
                        .truncate(true)
                        .open(&sedscript_path)
                        .map_err(From::from)
                } else {
                    Err(e).map_err(From::from)
                }
            },
        )?;
    rayon::spawn(move || {
        let mut wrote = false;
        for s in pending_changes_rx {
            wrote = true;
            pending_changes.write_all(&s.as_ref()).unwrap();
            pending_changes.flush().unwrap();
        }
        if !wrote {
            std::fs::remove_file(sedscript_path).unwrap();
        }
    });

    let limit = opt.limit.unwrap_or_default().wrapping_sub(1).saturating_add(1);
    let count = crossbeam_utils::atomic::AtomicCell::new(0usize);
    let things = urls
        .into_par_iter()
        .map(|mut sp: ytdl::StartingPoint| -> Result<Option<_>> {
            info!("read {:?}", sp);
            if opt.scrape {
                sp.scrape(pending_changes_tx.clone(), opt.cookies)?;
            }
            Ok(if opt.parse { sp.read()?.rzip(Some(sp)) } else { None })
        })
        .flat_map(Result::transpose)
        .filter(|r| if r.is_ok() { count.fetch_add(1) < limit } else { true })
        .collect::<Result<_>>()?;
    drop(pending_changes_tx);
    if opt.download {
        download_things(&things, opt.cookies, opt.interface.as_deref())?;
    } else {
        info!("Parsed {} items", things.len());
    }

    Ok(())
}
