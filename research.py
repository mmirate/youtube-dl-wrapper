import os
import time
import json
import requests
from plumbum.cmd import youtube_dl
from plumbum import BG
from plumbum.cmd import aria2c

pid = os.getpid()

aria2c['--enable-rpc', '--auto-file-renaming=false', '--continue=true', '--max-download-limit=16', f'--stop-with-process={pid}'] & BG

resp = requests.post("http://localhost:6800/jsonrpc", json={
    'jsonrpc': '2.0', 'id': 1,
    'method': 'aria2.addUri',
    'params': [['http://example.com'], {"gid": "ffffffffffffffff"}]}).json()
print(resp)

gid = resp.get('result', None)

resp = requests.post("http://localhost:6800/jsonrpc", json={
    'jsonrpc': '2.0', 'id': 4,
    'method': 'aria2.addUri',
    'params': [['http://example.com'], {"gid": "fffffffffffffffe"}]}).json()
print(resp)

if gid:
    while True:
        resp = requests.post("http://localhost:6800/jsonrpc", json={
            'jsonrpc': '2.0', 'id': 2,
            'method': 'aria2.tellStatus',
            'params': [gid,
                       ['gid',
                        'status',
                        'totalLength',
                        'completedLength',
                        'errorCode',
                        'errorMessage']]}).json()
        print(resp)
        if resp.get('result', {}).get('status', "") not in ("waiting", "active"):
            break
        else:
            time.sleep(1)

os.unlink('index.html')

resp = requests.post("http://localhost:6800/jsonrpc", json={
    'jsonrpc': '2.0', 'id': 3,
    'method': 'aria2.tellStopped',
    'params': [0, 100,
                ['gid',
                'status',
                'totalLength',
                'completedLength',
                'errorCode',
                'errorMessage']]}).json()
print(resp)

resp = requests.post("http://localhost:6800/jsonrpc", json={
    'jsonrpc': '2.0', 'id': 4,
    'method': 'aria2.addUri',
    'params': [['http://example.com/nx'], {"gid": "ffffffffffffffff"}]}).json()
print(resp)


# youtube-dl -f 'bestaudio[protocol!=http_dash_segments]' -J \
# 'https://www.youtube.com/channel/UCj7ifdp2pMOYkpxAPml8VXQ/playlists?view=50&sort=dd&shelf_id=3579731707244514319'

# json.dump(json.loads(
#     youtube_dl('-f',
#                'bestaudio[protocol!=http_dash_segments]', '-J',
#                '--youtube-skip-dash-manifest',
#                'https://www.youtube.com/channel/UCj7ifdp2pMOYkpxAPml8VXQ/playlists?view=50&sort=dd&shelf_id=3579731707244514319')),
#           open('mance4.json', 'w'), sort_keys=True, indent=4)


# json.dump(
#     json.load(
#         open('mance4.after.json', 'r', encoding='utf-8')
#     ),
#     open('mance4.after.pretty.json', 'w', encoding='utf-8'),
#     sort_keys=True, indent=4)

# json.dump(json.loads(
#     youtube_dl('-f',
#                'bestaudio[protocol!=http_dash_segments]', '-J',
#                '--youtube-skip-dash-manifest',
#                'https://www.youtube.com/playlist?list=OLAK5uy_kjOvGmjtouCylHhHAGteT9TtP5p-PqBg8')),
#           open('mance5.json', 'w'), sort_keys=True, indent=4)

# json.dump(json.loads(
#     youtube_dl('-f',
#                'bestaudio[protocol!=http_dash_segments]', '-iJ',
#                '--match-filter', "title != 'Top Tracks - Billy Higgins'",
#                '--youtube-skip-dash-manifest', 'https://www.youtube.com/channel/UC_fppnH1UvSuP3-LGeJfGYw/playlists', retcode=None)), # look ma, no shelf id!
#           open('higgins2.json', 'w'), sort_keys=True, indent=4)

# import sys

# json.dump(json.loads(
#     youtube_dl('-f',
#                'bestaudio[protocol!=http_dash_segments]', '-iJ',
#                '--flat-playlist',
#                '--youtube-skip-dash-manifest', 'https://www.youtube.com/channel/UCj7ifdp2pMOYkpxAPml8VXQ/playlists?view=50&sort=dd&shelf_id=3579731707244514319', retcode=None)),
#           sys.stdout, sort_keys=True, indent=4)