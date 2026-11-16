#!/usr/bin/env python3
"""Builds public/index.html from the game source (game.src.html)."""
import re, pathlib
root = pathlib.Path(__file__).parent
src = (root / 'game.src.html').read_text()

def sub(old, new, count=1):
    global src
    assert src.count(old) == count, (old[:50], src.count(old))
    src = src.replace(old, new)

sub('<meta name="claude:viewport-fit-sides" content="cover">\n',
    '<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
    '<meta name="theme-color" content="#0b0907">\n<meta name="description" content="Six Chambers - a first-person 3D Russian-roulette game for 2-4 players, with online rooms.">\n'
    '<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 viewBox=%270 0 32 32%27%3E%3Ccircle cx=%2716%27 cy=%2716%27 r=%2714%27 fill=%27%23160f0d%27 stroke=%27%23b3261e%27 stroke-width=%272%27/%3E%3Ccircle cx=%2716%27 cy=%2716%27 r=%274%27 fill=%27%23b3261e%27/%3E%3C/svg%3E">\n'
    '<style>*,*::before,*::after{box-sizing:border-box}html,body{margin:0;min-height:100%}[hidden]{display:none!important}'
    ':root{color-scheme:light;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}img{max-width:100%}body{font:14px system-ui,sans-serif}</style>\n')

sub("(window.claude && window.claude.use) ? await window.claude.use('room') : null",
    "(window.SixSocket ? await window.SixSocket.room() : null)")
sub("Online play isn't available in this view. The owner has to share the page with you (Contributor access or higher), and you need to open it signed in.",
    "Can't reach the game server. Check your connection and try again.")
sub('<p class="fine">Friends need this page shared with them (Contributor access or higher) and must open it signed in.</p>',
    '<p class="fine">Share the room code with friends - they open this same site and join with it.</p>')

# adapter before the last <script>
import hashlib, re as _re
adapter = (root / 'public' / 'socket-room.js').read_text()
ver = hashlib.sha1(adapter.encode()).hexdigest()[:10]
# every SixSocket / SS member the game uses must exist in the adapter
used = set(_re.findall(r'\bSS\.(\w+)', src)) | set(_re.findall(r'SixSocket\.(\w+)', src)) | set(_re.findall(r'\bSS\.admin\.(\w+)', src))
missing = [u for u in sorted(used) if _re.search(r'\b' + u + r'\s*:', adapter) is None]
assert not missing, 'socket-room.js is missing: ' + ', '.join(missing)
i = src.rindex('<script>')
src = src[:i] + '<script src="socket-room.js?v=' + ver + '"></script>\n' + src[i:]
if '<!doctype' not in src[:20].lower():
    src = '<!doctype html>\n<html lang="en">\n<head>\n' + src
    # close head before <main
    j = src.index('<main')
    src = src[:j] + '</head>\n<body>\n' + src[j:] 
    src = src.rstrip() + '\n</body>\n</html>\n'
(root / 'public' / 'index.html').write_text(src)
print('ok', len(src))
