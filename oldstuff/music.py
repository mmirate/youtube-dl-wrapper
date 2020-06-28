#!/usr/bin/python3
from collections import deque
import os.path
import re
import eyed3
import sys
import json
from random import shuffle
from typing import List, Tuple
# from multiprocessing import Pool
from plumbum.cmd import youtube_dl, nq
from plumbum import cli, local, FG
# from plumbum import BG, TF, RETCODE
import plumbum
# from pprint import pprint


BASE = local.path(os.path.dirname(os.path.abspath(__file__)))
YOUTUBE_URL = 'https://www.youtube.com/watch?v={}'

youtube_dl_info = youtube_dl['--flat-playlist', '-J'] >= sys.stderr
youtube_dl_anger = youtube_dl['-xik', '--playlist-random', '-f', 'bestaudio', '--audio-format', 'mp3',
                              '--audio-quality', '0', '--add-metadata', '--download-archive', 'done.txt',
                              '--external-downloader', 'aria2c', '--write-info-json',
                              '--ffmpeg-location', BASE / 'ffmpeg']


def id_for(url: str) -> str:
    if not url:
        pass
    elif '#' in url or ';' in url:
        print("Skipping due to comment for {!r}".format(url), file=sys.stderr)
    else:
        try:
            return url.split("playlist?list=", maxsplit=1)[1]
        except IndexError:
            print("Skipping due to not-a-playlist for {!r}".format(url), file=sys.stderr)
    return None


def fix_tags(mp3: plumbum.LocalPath, album_title: str) -> None:
    f = eyed3.load(mp3)
    changed = False
    new_album = re.sub(r' - Topic$', '', (f.tag.album or '').strip() or album_title)
    if f.tag.album != new_album:
        f.tag.album = new_album
        changed = True
    if f.tag.artist and f.tag.artist.endswith(' - Topic'):
        f.tag.artist = re.sub(r' - Topic$', '', (f.tag.artist or '').strip())
        changed = True
    if changed:
        f.tag.save()


def get_playlist_info(url, cwd) -> dict:
    p = cwd / 'playlist.json'
    try:
        return json.load(p.open('r'))
    except Exception:
        ret = json.loads(youtube_dl_info(url))
        p.write(json.dumps(ret, indent=4), encoding='utf-8')
        return ret


def upsert_file(p, st, sentinel=None, **kwargs):
    return p.write(st(), **kwargs) if not p.is_file() else sentinel


def upsert_dir(p, sentinel=None):
    return p.mkdir() if not p.is_dir() else sentinel


def _download_playlist(url, cwd) -> None:
    # mp3_ids = set(x.basename[-4-11:-4] for x in cwd // "*.mp3")
    done_ids = set(x.split()[1] for x in (cwd / "done.txt").read(encoding='utf-8').split('\n') if x) if (cwd / "done.txt").is_file() else set()
    # pl_data = json.loads(youtube_dl_info(url))
    pl_data = get_playlist_info(url, cwd)
    pl_ids = set(x['url'] for x in pl_data['entries'])
    # if not (cwd / "todo.txt").is_file():
    #     (cwd / "todo.txt").write('\n'.join(sorted(pl_ids)), encoding='utf-8')
    sentinel = object()

    reject_ids = set(filter(bool, (BASE / "rejects.txt").read(encoding='utf-8').split('\n')))

    pl_ids -= reject_ids

    # upsert_file(cwd / "playlist.json", lambda: json.dumps(pl_data, indent=4), encoding='utf-8')
    upsert_file(cwd / "title.txt", lambda: pl_data['title'], encoding='utf-8')
    if upsert_file(cwd / "todo.txt", lambda: '\n'.join(sorted(pl_ids)), sentinel=sentinel, encoding='utf-8') is sentinel:
        old_todos = set(filter(bool, (cwd / "todo.txt").read(encoding='utf-8').split('\n')))
        old_todos -= reject_ids
        if pl_ids - old_todos:
            (cwd / "todo.txt").write('\n'.join(sorted(pl_ids | old_todos)), encoding='utf-8')

    todos = {k: YOUTUBE_URL.format(k) for k in pl_ids - done_ids}
    print("attempting to download %s items" % len(todos), file=sys.stderr)
    # futures = {}
    jobs = []
    todos_keys = list(todos.keys())
    shuffle(todos_keys)
    # for k in todos_keys:
    for i, k in enumerate(todos_keys):
        di = BASE / "nqd" / str(i % 8)
        # upsert_dir(di)
        jobs.append((di, nq['-c', youtube_dl_anger, todos[k]].with_env(NQDIR=di)()))
        # futures[k] = youtube_dl_anger[todos[k]] & BG(stdout=sys.stdout, stderr=sys.stderr, retcode=None)
        # if i > 4:
        #     open('/dev/null', 'w').write(str(futures[todos_keys[i - 4]].returncode))
    for d, i in jobs:
        nq['-w', i].with_env(NQDIR=d)()
    # for k in futures:
    #     if not futures[k].returncode:
    #         fix_tags((cwd // "*{}.mp3".format(k))[0], pl_data['title'])
    #         print("%s: success" % todos[k])
    #     else:
    #         print("%s: failure" % todos[k])
    for mp3 in cwd // "*.mp3":
        fix_tags(mp3, pl_data['title'])


def bgrun_list(ps: list, limit: int) -> None:
    if ps and limit:
        with ps.pop().bgrun(stdout=sys.stdout, stderr=sys.stderr) as p:
            bgrun_list(ps, limit - 1)
            p.run()


def read_playlists() -> 'List[Tuple[str, str]]':
    from operator import itemgetter
    # print(repr(downloader))
    # print(repr(str(downloader)))
    decorate = lambda f: lambda v: (v, f(v))  # noqa: E731
    playlists = list(filter(itemgetter(1), map(decorate(id_for), set(map(str.strip, (BASE / "music.txt").open('r'))))))
    shuffle(playlists)
    return playlists


class Orchestrator(cli.Application):
    def main(self, *args):
        if args:
            print("Unknown command {!r}".format(args[0]), file=sys.stderr)
            return 1
        elif self.nested_command:
            return None

        downloader = local[__file__]['playlist']
        waiters = deque()
        upsert_dir(BASE / "nqd")
        for i, (url, playlist_id) in enumerate(read_playlists()):
            d = BASE / playlist_id
            upsert_dir(d)
            # nqsd = BASE / "nqd" / str(i % 10)
            # upsert_dir(nqsd)
            with local.cwd(d):
                # if nq['-t'] & RETCODE:
                #     continue
                cmd = nq["-c", downloader, url].with_env(NQDIR=str(d))
                # print(repr(str(cmd)))
                # print(repr(cmd))
                cmd & FG
                waiters.insert(0, nq['-w'].with_env(NQDIR=str(d)))
            if len(waiters) >= 16:
                bgrun_list(waiters, 16 >> 1)
        print("%s waiters remain" % len(waiters), file=sys.stderr)
        bgrun_list(waiters, -1)
        return 0
        # import itertools as p
        # if True:
        # with Pool(100) as p:
        #     for x in p.imap(download_playlist, playlists):
        #         print(x)
        # input("<Enter> to continue...")


@Orchestrator.subcommand("fix_tags")
class TagFixer(cli.Application):
    def main(self):
        for _url, playlist_id in sorted(read_playlists()):
            d = BASE / playlist_id
            with local.cwd(d) as cwd:
                album_title = cwd / "title.txt"
                if not album_title.is_file():
                    continue
                album_title = album_title.read().strip()
                if not album_title:
                    continue
                for done_id in (
                    set(x.split()[1] for x in (cwd / "done.txt").read(encoding='utf-8').split('\n') if x) if (cwd / "done.txt").is_file() else set()
                ):
                    mp3s = d // "*{}.mp3".format(done_id)
                    for mp3 in mp3s:
                        fix_tags(mp3, album_title)
            print(d, file=sys.stderr)


@Orchestrator.subcommand("enumerate")
class Enumerator(cli.Application):
    def main(self, url):
        get_playlist_info(url, local.cwd)


@Orchestrator.subcommand("playlist")
class Downloader(cli.Application):
    def main(self, url):
        _download_playlist(url, local.cwd)
        sys.stdout.flush()
        sys.stderr.flush()


if __name__ == '__main__':
    Orchestrator.run()
