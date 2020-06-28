import pyperclip
from plumbum.cmd import youtube_dl, jq

pyperclip.copy((youtube_dl['--flat-playlist', '-J', pyperclip.paste()] | jq['-eM', '-r', '.entries[] | .url'])())
