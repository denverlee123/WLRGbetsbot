# Discord NFL Bets Bot

A Discord slash-command bot for tracking head-to-head NFL bets between friends. It:
- Calculates fantasy points (PPR/HALF/STD) and PPG
- Enforces a minimum offensive snap% (e.g., 25%)
- Posts **weekly auto-standings** (default Tuesday 12:00 PM America/Toronto)
- Lets you tag **participants** in each bet so everyone sees who’s in

## Quick Start

1) **Create a bot** in the Discord Developer Portal, copy the token. Invite it with the `applications.commands` scope.
2) Download this folder, copy `.env.example` to `.env`, and fill values.
3) Install & run:
```bash
pip install -r requirements.txt
python bot.py
```
4) In your server, use:
- `/addbet` — includes optional `participant1..participant6` user pickers
- `/standings`
- `/mybets`

## Notes
- Data comes from nflverse weekly player stats and PFR snap counts (updated nightly in-season).
- The bot auto-migrates your DB to add a `participants` column if missing.
