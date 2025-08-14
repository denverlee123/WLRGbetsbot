python
import os
import io
import time
import asyncio
import sqlite3
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import httpx
import pandas as pd
import pytz
from dotenv import load_dotenv

import discord
from discord import app_commands
from discord.ext import tasks

# ========= CONFIG =========
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")
WEEKLY_CHANNEL_ID = int(os.getenv("CHANNEL_ID_FOR_WEEKLY", "0"))

# Timezone for weekly post (America/Toronto as requested)
TZ = pytz.timezone("America/Toronto")
WEEKLY_POST_DAY = 1  # 0=Mon, 1=Tue, ... default: Tuesday
WEEKLY_POST_HOUR = 12  # 12:00 local time
WEEKLY_POST_MINUTE = 0

# Data URLs (nflverse). Updated nightly during season.
NFLVERSE_BASE = "https://github.com/nflverse/nflverse-data/releases/download"
SEASON = dt.datetime.now(TZ).year  # adjust automatically each year

PLAYER_STATS_URL = f"{NFLVERSE_BASE}/player_stats/stats_player_week_{SEASON}.csv"
SNAP_COUNTS_URL = f"https://github.com/nflverse/nflverse-pfr/releases/download/snap_counts/snap_counts_{SEASON}.csv"
PLAYERS_URL = f"{NFLVERSE_BASE}/players/players.csv"  # id map (GSIS <-> PFR, ESPN, etc.)

CACHE_DIR = ".cache"
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_TTL_HOURS = 12

# Default scoring configs
SCORING_PRESETS = {
    "PPR": {"receptions": 1.0, "pass_yd": 0.04, "pass_td": 4.0, "int": -2.0,
            "rush_yd": 0.1, "rush_td": 6.0, "rec_yd": 0.1, "rec_td": 6.0, "fumbles_lost": -2.0},
    "HALF": {"receptions": 0.5, "pass_yd": 0.04, "pass_td": 4.0, "int": -2.0,
             "rush_yd": 0.1, "rush_td": 6.0, "rec_yd": 0.1, "rec_td": 6.0, "fumbles_lost": -2.0},
    "STD": {"receptions": 0.0, "pass_yd": 0.04, "pass_td": 4.0, "int": -2.0,
            "rush_yd": 0.1, "rush_td": 6.0, "rec_yd": 0.1, "rec_td": 6.0, "fumbles_lost": -2.0}
}

# ========= DISCORD CLIENT =========
intents = discord.Intents.default()
intents.guilds = True

class BetBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        if GUILD_ID:
            self.tree.copy_global_to(guild=discord.Object(id=int(GUILD_ID)))
            await self.tree.sync(guild=discord.Object(id=int(GUILD_ID)))
        else:
            await self.tree.sync()

bot = BetBot()

# ========= PERSISTENCE =========
DB = "bets.sqlite"

def db_init():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_discord_id TEXT NOT NULL,
        player_a TEXT NOT NULL,
        player_b TEXT NOT NULL,
        description TEXT,
        scoring TEXT NOT NULL,
        min_snap_pct REAL NOT NULL,
        season INTEGER NOT NULL,
        start_week INTEGER DEFAULT 1,
        end_week INTEGER DEFAULT 18,
        participants TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    )
    """)
    # Safe migration if existing DB lacks participants column
    try:
        cur.execute("SELECT participants FROM bets LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE bets ADD COLUMN participants TEXT DEFAULT ''")
    con.commit()
    con.close()

db_init()

# ========= DATA LAYER =========
async def fetch_csv(url: str, cache_name: str) -> pd.DataFrame:
    """Fetch CSV with naive caching."""
    cache_path = os.path.join(CACHE_DIR, cache_name)
    use_cache = False
    if os.path.exists(cache_path):
        age_hours = (time.time() - os.path.getmtime(cache_path)) / 3600
        if age_hours < CACHE_TTL_HOURS:
            use_cache = True
    if use_cache:
        return pd.read_csv(cache_path)
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url)
        r.raise_for_status()
        with open(cache_path, "wb") as f:
            f.write(r.content)
        return pd.read_csv(io.BytesIO(r.content))

async def load_player_stats() -> pd.DataFrame:
    return await fetch_csv(PLAYER_STATS_URL, f"player_stats_{SEASON}.csv")

async def load_snap_counts() -> pd.DataFrame:
    return await fetch_csv(SNAP_COUNTS_URL, f"snap_counts_{SEASON}.csv")

async def load_players_map() -> pd.DataFrame:
    return await fetch_csv(PLAYERS_URL, f"players_all.csv")

def fantasy_points(row: pd.Series, scoring: Dict[str, float]) -> float:
    pts = 0.0
    g = lambda k: float(row.get(k, 0) or 0)
    pts += g("receptions") * scoring["receptions"]
    pts += g("passing_yards") * scoring["pass_yd"]
    pts += g("passing_tds") * scoring["pass_td"]
    pts += g("interceptions") * scoring["int"]
    pts += g("rushing_yards") * scoring["rush_yd"]
    pts += g("rushing_tds") * scoring["rush_td"]
    pts += g("receiving_yards") * scoring["rec_yd"]
    pts += g("receiving_tds") * scoring["rec_td"]
    pts += g("fumbles_lost") * scoring["fumbles_lost"]
    return round(pts, 2)

async def compute_ppg(player_name: str, start_week: int, end_week: int, min_snap_pct: float, scoring_key: str) -> Tuple[float, int]:
    """
    Returns (ppg, n_games_qualified)
    """
    scoring = SCORING_PRESETS[scoring_key]
    stats = await load_player_stats()
    snaps = await load_snap_counts()
    players = await load_players_map()

    stats = stats[(stats["season"] == SEASON) & (stats["season_type"] == "REG")]
    stats = stats[(stats["week"] >= start_week) & (stats["week"] <= end_week)]

    mask = stats["player_name"].str.lower().str.contains(player_name.lower())
    cand = stats[mask].copy()
    if cand.empty:
        return (0.0, 0)

    players_small = players[["gsis_id", "display_name", "pfr_id"]].rename(
        columns={"display_name": "player_name_map"}
    )
    cand = cand.merge(players_small, left_on="player_id", right_on="gsis_id", how="left")

    snaps_small = snaps[["season", "week", "team", "player", "pfr_player_id", "offense_pct"]].copy()

    joined = cand.merge(
        snaps_small,
        left_on=["season", "week", "team", "pfr_id"],
        right_on=["season", "week", "team", "pfr_player_id"],
        how="left"
    )

    missing = joined["offense_pct"].isna()
    if missing.any():
        fallback = cand[missing].merge(
            snaps_small,
            left_on=["season", "week", "team", "player_name"],
            right_on=["season", "week", "team", "player"],
            how="left"
        )
        joined.loc[missing, "offense_pct"] = fallback["offense_pct"].values

    joined["offense_pct"] = joined["offense_pct"].fillna(0.0)
    qualified = joined[joined["offense_pct"] >= float(min_snap_pct)].copy()
    if qualified.empty:
        return (0.0, 0)

    qualified["fp"] = qualified.apply(lambda r: fantasy_points(r, scoring), axis=1)
    ppg = qualified["fp"].mean()
    n = qualified.shape[0]
    return (round(ppg, 2), int(n))

def fmt_ppg(ppg: float, n: int) -> str:
    return f"{ppg:.2f} PPG over {n} qualifying game{'s' if n!=1 else ''}"

# ========= HELPERS =========
def _collect_participant_ids(*users: Optional[discord.User]) -> List[str]:
    ids = []
    for u in users:
        if u is not None:
            ids.append(str(u.id))
    # dedupe preserve order
    seen = set()
    out = []
    for i in ids:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out

async def get_current_max_week() -> int:
    """Look at what's published for this REG season and return the max week available (0 if none)."""
    try:
        stats = await load_player_stats()
        stats = stats[(stats["season"] == SEASON) & (stats["season_type"] == "REG")]
        if stats.empty:
            return 0
        return int(stats["week"].max())
    except Exception:
        return 0

async def close_completed_bets():
    """Auto-close bets whose end_week has passed according to published data (regular season)."""
    max_week = await get_current_max_week()
    if max_week <= 0:
        return
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("UPDATE bets SET is_active=0 WHERE season=? AND is_active=1 AND end_week<=?", (SEASON, max_week))
    con.commit()
    con.close()

def user_can_edit(interaction: discord.Interaction, creator_id: str) -> bool:
    if str(interaction.user.id) == creator_id:
        return True
    perms = interaction.user.guild_permissions
    return bool(perms.manage_guild or perms.administrator)

# ========= COMMANDS =========
@bot.tree.command(name="addbet", description="Create a new head-to-head bet")
@app_commands.describe(
    player_a="Player A (e.g., 'CeeDee Lamb')",
    player_b="Player B (e.g., 'Amon-Ra St. Brown')",
    participant1="Tag a participant in the bet",
    participant2="Tag a participant in the bet",
    participant3="Tag a participant in the bet",
    participant4="Tag a participant in the bet",
    participant5="Tag a participant in the bet",
    participant6="Tag a participant in the bet",
    scoring="Scoring: PPR, HALF, STD",
    min_snap_pct="Min offensive snap% for a game to count (e.g., 25)",
    start_week="Start week (default 1)",
    end_week="End week (default 18)",
    description="Short description of the bet"
)
async def addbet(
    interaction: discord.Interaction,
    player_a: str,
    player_b: str,
    participant1: Optional[discord.User] = None,
    participant2: Optional[discord.User] = None,
    participant3: Optional[discord.User] = None,
    participant4: Optional[discord.User] = None,
    participant5: Optional[discord.User] = None,
    participant6: Optional[discord.User] = None,
    scoring: str = "PPR",
    min_snap_pct: float = 25.0,
    start_week: int = 1,
    end_week: int = 18,
    description: str = ""
):
    scoring = scoring.upper()
    if scoring not in SCORING_PRESETS:
        await interaction.response.send_message("Scoring must be one of: PPR, HALF, STD", ephemeral=True)
        return
    if not (0 <= min_snap_pct <= 100):
        await interaction.response.send_message("min_snap_pct must be between 0 and 100", ephemeral=True)
        return
    # Force end at end of regular season (18)
    end_week = min(int(end_week), 18)
    start_week = max(1, int(start_week))

    participant_ids = _collect_participant_ids(participant1, participant2, participant3, participant4, participant5, participant6)
    participant_ids_str = ",".join(participant_ids)

    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO bets(creator_discord_id, player_a, player_b, description, scoring, min_snap_pct, season, start_week, end_week, participants, is_active, created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,1,?)
    """, (
        str(interaction.user.id),
        player_a.strip(),
        player_b.strip(),
        description.strip(),
        scoring,
        float(min_snap_pct),
        SEASON,
        start_week,
        end_week,
        participant_ids_str,
        dt.datetime.now(TZ).isoformat()
    ))
    con.commit()
    bet_id = cur.lastrowid
    con.close()

    mention_str = " ".join(f"<@{uid}>" for uid in participant_ids) if participant_ids else "None tagged"
    await interaction.response.send_message(
        f"✅ Bet #{bet_id} created: **{player_a} vs {player_b}** ({scoring}, ≥{min_snap_pct}% snaps, Weeks {start_week}-{end_week}). "
        f"Participants: {mention_str}\n{description}"
    )

@bot.tree.command(name="editbet", description="Edit an existing bet you created (or manage server)")
@app_commands.describe(
    bet_id="ID of the bet to edit",
    player_a="New Player A (leave blank to keep)",
    player_b="New Player B (leave blank to keep)",
    scoring="New scoring: PPR, HALF, STD",
    min_snap_pct="New minimum snap%",
    start_week="New start week (1-18)",
    end_week="New end week (1-18, ends at regular season)",
    description="New description",
    participant1="Replace participants: tag up to 6, leave all blank to keep current",
    participant2="",
    participant3="",
    participant4="",
    participant5="",
    participant6="",
    clear_participants="If true, clears all current participants"
)
async def editbet(
    interaction: discord.Interaction,
    bet_id: int,
    player_a: Optional[str] = None,
    player_b: Optional[str] = None,
    scoring: Optional[str] = None,
    min_snap_pct: Optional[float] = None,
    start_week: Optional[int] = None,
    end_week: Optional[int] = None,
    description: Optional[str] = None,
    participant1: Optional[discord.User] = None,
    participant2: Optional[discord.User] = None,
    participant3: Optional[discord.User] = None,
    participant4: Optional[discord.User] = None,
    participant5: Optional[discord.User] = None,
    participant6: Optional[discord.User] = None,
    clear_participants: Optional[bool] = False
):
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT creator_discord_id, is_active FROM bets WHERE id=?", (bet_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        await interaction.response.send_message("Bet not found.", ephemeral=True)
        return
    creator_id, is_active = row
    if not user_can_edit(interaction, creator_id):
        con.close()
        await interaction.response.send_message("You don't have permission to edit this bet.", ephemeral=True)
        return

    updates = []
    params = []

    if player_a:
        updates.append("player_a=?"); params.append(player_a.strip())
    if player_b:
        updates.append("player_b=?"); params.append(player_b.strip())

    if scoring:
        scoring = scoring.upper()
        if scoring not in SCORING_PRESETS:
            con.close()
            await interaction.response.send_message("Scoring must be one of: PPR, HALF, STD", ephemeral=True)
            return
        updates.append("scoring=?"); params.append(scoring)

    if min_snap_pct is not None:
        if not (0 <= float(min_snap_pct) <= 100):
            con.close()
            await interaction.response.send_message("min_snap_pct must be between 0 and 100", ephemeral=True)
            return
        updates.append("min_snap_pct=?"); params.append(float(min_snap_pct))

    if start_week is not None:
        start_week = max(1, min(18, int(start_week)))
        updates.append("start_week=?"); params.append(start_week)

    if end_week is not None:
        end_week = max(1, min(18, int(end_week)))
        updates.append("end_week=?"); params.append(end_week)

    if description is not None:
        updates.append("description=?"); params.append(description.strip())

    # Participants replacement
    provided_participants = any(p is not None for p in [participant1, participant2, participant3, participant4, participant5, participant6])
    if clear_participants or provided_participants:
        if clear_participants:
            participants_str = ""
        else:
            ids = _collect_participant_ids(participant1, participant2, participant3, participant4, participant5, participant6)
            participants_str = ",".join(ids)
        updates.append("participants=?"); params.append(participants_str)

    if not updates:
        con.close()
        await interaction.response.send_message("Nothing to update. Provide at least one field.", ephemeral=True)
        return

    params.append(bet_id)
    cur.execute(f"UPDATE bets SET {', '.join(updates)} WHERE id=?", params)
    con.commit()
    con.close()

    await interaction.response.send_message(f"✅ Bet #{bet_id} updated.")

@bot.tree.command(name="standings", description="Show current standings across all active bets")
async def standings(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    await close_completed_bets()
    max_week = await get_current_max_week()

    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT id, player_a, player_b, scoring, min_snap_pct, start_week, end_week, description, participants FROM bets WHERE is_active=1 AND season=?", (SEASON,))
    rows = cur.fetchall()
    con.close()

    if not rows:
        await interaction.followup.send("No active bets yet. Use `/addbet` to create one!")
        return

    lines = []
    for (bid, a, b, scoring, minpct, s, e, desc, participants_str) in rows:
        # Clamp computation to available published weeks
        end_w = min(e, max_week if max_week > 0 else e)
        a_ppg, a_n = await compute_ppg(a, s, end_w, minpct, scoring)
        b_ppg, b_n = await compute_ppg(b, s, end_w, minpct, scoring)
        leader = "TIED"
        if a_ppg > b_ppg: leader = f"{a} ↑"
        elif b_ppg > a_ppg: leader = f"{b} ↑"
        parts = " ".join(f"<@{uid}>" for uid in (participants_str or "").split(",") if uid)
        lines.append(
            f"**#{bid}** {a} vs {b} — {leader}\n"
            f"• {a}: {fmt_ppg(a_ppg, a_n)}\n"
            f"• {b}: {fmt_ppg(b_ppg, b_n)}\n"
            f"• {scoring}, ≥{minpct}% snaps, Weeks {s}-{e} {f'— {desc}' if desc else ''}\n"
            f"• Participants: {parts if parts else '—'}"
        )

    chunk = "\n\n".join(lines[:15])
    title_week = max_week if max_week > 0 else "—"
    embed = discord.Embed(
        title=f"Bet Standings — {SEASON} (Through Week {title_week})",
        description=chunk,
        color=discord.Color.blurple()
    )
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="mybets", description="List the bets you created")
async def mybets(interaction: discord.Interaction):
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT id, player_a, player_b, scoring, min_snap_pct, start_week, end_week, description, participants, is_active FROM bets WHERE creator_discord_id=? AND season=?", (str(interaction.user.id), SEASON))
    rows = cur.fetchall()
    con.close()
    if not rows:
        await interaction.response.send_message("You don’t have any bets.", ephemeral=True)
        return
    lines = []
    for (bid,a,b,scoring,minpct,s,e,d,participants_str,is_active) in rows:
        parts = " ".join(f"<@{uid}>" for uid in (participants_str or "").split(",") if uid)
        status = "ACTIVE" if is_active else "CLOSED"
        lines.append(f"**#{bid}** [{status}] {a} vs {b} ({scoring}, ≥{minpct}% snaps, W{s}-{e}) {f'— {d}' if d else ''}\n• Participants: {parts if parts else '—'}")
    await interaction.response.send_message("\n\n".join(lines), ephemeral=True)

# ========= WEEKLY AUTO-POST =========
def next_run_datetime() -> dt.datetime:
    now_local = dt.datetime.now(TZ)
    days_ahead = (WEEKLY_POST_DAY - now_local.weekday()) % 7
    candidate = (now_local + dt.timedelta(days=days_ahead)).replace(hour=WEEKLY_POST_HOUR, minute=WEEKLY_POST_MINUTE, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += dt.timedelta(days=7)
    return candidate

@tasks.loop(seconds=60)
async def weekly_job():
    now = dt.datetime.now(TZ)
    target = weekly_job.next_target
    if abs((now - target).total_seconds()) <= 30:
        try:
            # Close completed bets before posting
            await close_completed_bets()
            max_week = await get_current_max_week()
            channel = bot.get_channel(WEEKLY_CHANNEL_ID)
            if channel is not None:
                con = sqlite3.connect(DB)
                cur = con.cursor()
                cur.execute("SELECT id, player_a, player_b, scoring, min_snap_pct, start_week, end_week, description, participants, is_active FROM bets WHERE season=?", (SEASON,))
                rows = cur.fetchall()
                con.close()
                if not rows:
                    await channel.send("No bets yet. Use `/addbet` to create one!")
                else:
                    lines = []
                    for (bid, a, b, scoring, minpct, s, e, desc, participants_str, is_active) in rows:
                        end_w = min(e, max_week if max_week > 0 else e)
                        a_ppg, a_n = await compute_ppg(a, s, end_w, minpct, scoring)
                        b_ppg, b_n = await compute_ppg(b, s, end_w, minpct, scoring)
                        leader = "TIED"
                        delta = round(a_ppg - b_ppg, 2)
                        if a_ppg > b_ppg: leader = f"{a} by {abs(delta):.2f}"
                        elif b_ppg > a_ppg: leader = f"{b} by {abs(delta):.2f}"
                        parts = " ".join(f"<@{uid}>" for uid in (participants_str or "").split(",") if uid)
                        status = "ACTIVE" if is_active else "CLOSED"
                        lines.append(f"**#{bid}** [{status}] {a} vs {b}: {leader} — {scoring}, ≥{minpct}% snaps (W{s}-{e}) • {parts if parts else ''}")
                    embed = discord.Embed(
                        title=f"Weekly Bet Standings — {SEASON} (Through Week {max_week if max_week>0 else '—'})",
                        description="\n".join(lines[:20]),
                        color=discord.Color.green()
                    )
                    await channel.send(embed=embed)
        finally:
            weekly_job.next_target = next_run_datetime()

@weekly_job.before_loop
async def before_weekly():
    await bot.wait_until_ready()
    weekly_job.next_target = next_run_datetime()

# ========= RUN =========
if __name__ == "__main__":
    weekly_job.start()
    bot.run(DISCORD_TOKEN)
