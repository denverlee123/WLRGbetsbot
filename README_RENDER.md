# WLRG Discord NFL Bets Bot — Render One-Click

This repo is ready for **one-click deploy on Render** as a Background Worker.

## One-Click Deploy

1. Push this folder to a GitHub repo (public or private linked to Render).
2. Open this link, replacing `<YOUR_REPO_URL>` with your GitHub repo URL:
   https://render.com/deploy?repo=<YOUR_REPO_URL>
3. Render will read `render.yaml` and create a **Worker** service.
4. Set these environment variables in the service settings:
   - DISCORD_TOKEN — your bot token
   - CHANNEL_ID_FOR_WEEKLY — Discord channel ID for Tuesday standings
   - GUILD_ID — optional, your server ID for faster slash command sync
5. Click **Create Resources** → after build, the worker starts and your bot goes online.

## Notes
- No web port is required; this runs as a **Background Worker**.
- Update weekly post time in `bot.py` via WEEKLY_POST_DAY/HOUR/MINUTE.
- Timezone defaults to America/Toronto.

## Local Dev
pip install -r requirements.txt
cp .env.example .env  # fill values
python bot.py
