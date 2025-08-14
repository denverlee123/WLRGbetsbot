# WLRG Discord NFL Bets Bot — Railway One‑Click

This repo is ready for **one‑click deploy on Railway**.

## One‑Click Deploy

1. Push this folder to a new **public GitHub repo** (or private linked to Railway).
2. Click this link (replace `<YOUR_REPO_URL>` with your GitHub repo URL):

   **https://railway.app/new?template=<YOUR_REPO_URL>**

   Railway will clone the repo and detect Python automatically.

3. When prompted, set environment variables:
   - `DISCORD_TOKEN` — your bot token
   - `CHANNEL_ID_FOR_WEEKLY` — channel ID for Tuesday standings posts
   - `GUILD_ID` — (optional) your server ID

4. Click **Deploy**. After the build finishes, the worker starts and the bot comes online.

> If you want a README button inside your repo, add this markdown and replace `<YOUR_REPO_URL>`:
>
> ```md
> [![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new?template=<YOUR_REPO_URL>)
> ```

## Notes
- Process type is a **worker** via `Procfile`, so no web port is required.
- `railway.json` sets `startCommand` to `python bot.py` and a safe restart policy.
- Update weekly post time in `bot.py` (`WEEKLY_POST_DAY/HOUR/MINUTE`).
- Timezone defaults to `America/Toronto`.

## Local Dev
```bash
pip install -r requirements.txt
cp .env.example .env  # fill values
python bot.py
```
