"""
Extract ESPN Fantasy Baseball box scores and load raw JSON into Snowflake.

Pulls daily box score data for a given matchup period, serializes each
scoring period's results as JSON, and inserts into ESPN_FANTASY.RAW.BOX_SCORES.
"""

import json
import os
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from espn_api.baseball import League
import snowflake.connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

ESPN_S2 = os.getenv("ESPN_S2")
SWID = os.getenv("SWID")
LEAGUE_ID = int(os.getenv("LEAGUE_ID"))
YEAR = 2026

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}

# ---------------------------------------------------------------------------
# Schedule loading
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "config", "matchup_schedule.json")


def load_schedule():
    """
    Load the matchup schedule from the external JSON config.
    Returns (season_opener, matchups) where matchups is a list of
    (matchup_period, start_date, end_date) tuples.
    """
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    season_opener = date.fromisoformat(config["season_opener"])

    matchups = []
    for m in config["matchups"]:
        matchups.append((
            m["matchup_period"],
            date.fromisoformat(m["start"]),
            date.fromisoformat(m["end"]),
        ))

    return season_opener, matchups


def date_to_scoring_period(d, season_opener):
    """Convert a calendar date to an ESPN scoring period number."""
    return (d - season_opener).days + 1


def get_scoring_periods(matchup_period, season_opener, matchups):
    """Return the list of scoring periods for a given matchup period."""
    for mp, start, end in matchups:
        if mp == matchup_period:
            num_days = (end - start).days + 1
            first_sp = date_to_scoring_period(start, season_opener)
            return list(range(first_sp, first_sp + num_days))
    raise ValueError(f"Matchup period {matchup_period} not found in schedule.")


# ---------------------------------------------------------------------------
# ESPN extraction
# ---------------------------------------------------------------------------
def connect_espn():
    """Authenticate and return the ESPN league object."""
    return League(
        league_id=LEAGUE_ID,
        year=YEAR,
        espn_s2=ESPN_S2,
        swid=SWID,
    )


def serialize_box_scores(league, scoring_period, matchup_period):
    """
    Pull box scores for a single scoring period and return a list of
    serialized matchup dicts.

    Both scoring_period AND matchup_period must be passed to the ESPN API
    to get historical player-level stats. Passing scoring_period alone
    returns today's stats regardless of which period was requested.
    """
    box_scores = league.box_scores(
        matchup_period=matchup_period,
        scoring_period=scoring_period,
    )
    matchups = []

    for matchup in box_scores:
        matchup_dict = {
            "home_team": matchup.home_team.team_name,
            "home_team_id": matchup.home_team.team_id,
            "away_team": matchup.away_team.team_name,
            "away_team_id": matchup.away_team.team_id,
            "home_score": matchup.home_score,
            "away_score": matchup.away_score,
            "home_lineup": [],
            "away_lineup": [],
        }

        for side in ["home", "away"]:
            lineup = getattr(matchup, f"{side}_lineup")
            lineup_list = []
            for player in lineup:
                period_stats = player.stats.get(scoring_period, {})
                player_dict = {
                    "name": player.name,
                    "playerId": player.playerId,
                    "position": player.position,
                    "lineupSlot": player.lineupSlot,
                    "proTeam": player.proTeam,
                    "points": period_stats.get("points", 0),
                    "breakdown": period_stats.get("breakdown", {}),
                }
                lineup_list.append(player_dict)
            matchup_dict[f"{side}_lineup"] = lineup_list

        matchups.append(matchup_dict)

    return matchups


# ---------------------------------------------------------------------------
# Snowflake loading
# ---------------------------------------------------------------------------
def load_to_snowflake(records, matchup_period):
    """
    Insert raw box score JSON records into Snowflake.
    Creates the target table if it doesn't exist.
    Deletes any existing data for this matchup period first (idempotent reload).
    """
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS BOX_SCORES (
                scoring_period  INTEGER,
                matchup_period  INTEGER,
                raw_json        VARIANT,
                loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        # Delete existing data for this matchup period so re-runs are idempotent
        cursor.execute(
            "DELETE FROM BOX_SCORES WHERE matchup_period = %s",
            (matchup_period,)
        )

        for record in records:
            cursor.execute(
                """
                INSERT INTO BOX_SCORES (scoring_period, matchup_period, raw_json)
                SELECT %s, %s, PARSE_JSON(%s)
                """,
                (
                    record["scoring_period"],
                    record["matchup_period"],
                    json.dumps(record["matchups"]),
                ),
            )

        conn.commit()
        print(f"  Loaded {len(records)} scoring periods into Snowflake.")

    finally:
        cursor.close()
        conn.close()


def extract_matchup_period(league, matchup_period):
    """
    Extract all scoring periods for a matchup period and load to Snowflake.
    """
    season_opener, matchups = load_schedule()
    scoring_periods = get_scoring_periods(matchup_period, season_opener, matchups)

    print(f"  Matchup period {matchup_period} spans {len(scoring_periods)} days "
          f"(scoring periods {scoring_periods[0]}-{scoring_periods[-1]})")

    records = []
    for sp in scoring_periods:
        print(f"  Pulling scoring period {sp}...")
        matchup_data = serialize_box_scores(league, sp, matchup_period)
        records.append({
            "scoring_period": sp,
            "matchup_period": matchup_period,
            "matchups": matchup_data,
        })

    load_to_snowflake(records, matchup_period)


def get_recent_matchup_periods(lookback_days=21):
    """
    Return matchup periods whose end date falls within the last
    `lookback_days` days (inclusive of today).

    This means:
    - Completed matchup periods are re-extracted (catches scoring adjustments)
    - Very old periods are skipped (no unnecessary API calls)
    - The current in-progress period is included if its end date is within range
    """
    season_opener, matchups = load_schedule()
    today = date.today()
    cutoff = today - timedelta(days=lookback_days)

    recent = []
    for mp, start, end in matchups:
        if end >= cutoff and end <= today:
            recent.append(mp)

    return sorted(recent)


if __name__ == "__main__":
    import sys

    # Usage:
    #   py extract/extract_box_scores.py          -> extract recent matchup periods (last 21 days)
    #   py extract/extract_box_scores.py 2         -> extract a specific matchup period
    #   py extract/extract_box_scores.py 1 2 3     -> extract multiple specific matchup periods

    if len(sys.argv) > 1:
        periods = [int(x) for x in sys.argv[1:]]
        print(f"Extracting specified matchup periods: {periods}")
    else:
        periods = get_recent_matchup_periods()
        if not periods:
            print("No completed matchup periods found in the last 21 days.")
            sys.exit(0)
        print(f"Extracting recent matchup periods: {periods}")

    league = connect_espn()

    for mp in periods:
        print(f"\nMatchup period {mp}:")
        extract_matchup_period(league, mp)

    print("\nDone.")