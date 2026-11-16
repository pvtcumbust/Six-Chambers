# Six Chambers

3D first-person Russian-roulette game for 2-4 players (vs House, pass & play, or online rooms).
No dependencies. One small Node server serves the page and relays online-room messages over WebSocket.

## Run locally
    node server.js            # http://localhost:3000   (Node 18+)
Open it in two browser tabs to try online: Host in one, join with the 4-letter code in the other.

## Publish (easiest: Render, free tier)
1. Push this folder to a GitHub repo.
2. render.com -> New -> Web Service -> pick the repo.
   Runtime: Node. Build command: (empty). Start command: `node server.js`.
3. Deploy. You get https://your-app.onrender.com (HTTPS + WebSockets work automatically).
4. Custom domain: Render -> Settings -> Custom Domains, then add the CNAME at your registrar.
Free instances sleep when idle; the first visit after a pause takes ~30s to wake.

Other hosts: Railway / Fly.io (use the Dockerfile) or any VPS (`node server.js` behind Caddy/nginx
with WebSocket proxying on /ws, and set TRUST_PROXY=1).

## Admin menu (secret)
Set an `ADMIN_TOKEN` environment variable (a long random string; Render -> Environment). Without it, admin tools are off.
In the game press **Ctrl+Alt+Shift+A**, enter the token, and you get:
- Rig next pull (secret): force the next pull Safe or Live, for anyone or a chosen seat. Peek shows the next chamber (offline or when you host).
- Bullets 1-5: reloads the gun now (players see the new count).
- Add bot: seats a bot while a match is running (up to 4 seats).
- Shotgun: pick a seat and blast them (you appear as the shooter when you're seated).
- Rooms: list live rooms and target any of them, even one you're not in.
- Broadcast: pops a banner on everyone connected to the site.
The server checks the token (rate-limited, 5 wrong tries = 10 min lockout per IP); players cannot fake these messages.
Broadcast reaches everyone connected to this one server instance, so run a single instance.

## Environment variables
- ADMIN_TOKEN  enables the admin menu
- PORT (default 3000)
- ALLOWED_ORIGINS  comma-separated origins allowed to open /ws, e.g. https://sixchambers.com
  (set this once you have a domain)
- TRUST_PROXY=1    when behind a proxy/load balancer, so per-IP limits use the real client IP

## Editing the game
public/index.html is built from game.src.html by `python3 build.py`
(adds the WebSocket adapter public/socket-room.js). Edit game.src.html, rebuild, redeploy.
The game server keeps nothing on disk; rooms live in memory and vanish on restart.
