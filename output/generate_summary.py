"""
Generate the weekly front-page summary from the mart tables.

Reads fct_weekly_team_scores and int_weekly_matchups to produce
a BBCode-formatted summary for the ESPN league front page.
"""

import os

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": "ANALYTICS",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}


def query_snowflake(sql, params=None):
    """Run a query and return results as a list of dicts."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        columns = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def get_weekly_scores(matchup_period=None):
    """Fetch weekly team scores. Defaults to most recent matchup period."""
    if matchup_period is None:
        result = query_snowflake(
            "SELECT MAX(matchup_period) as mp FROM fct_weekly_team_scores"
        )
        matchup_period = result[0]['mp']

    scores = query_snowflake("""
        SELECT matchup_period, team_name, total_points,
               hitting_points, pitching_points, days_in_period
        FROM fct_weekly_team_scores
        WHERE matchup_period = %s
        ORDER BY total_points DESC
    """, (matchup_period,))

    return matchup_period, scores


def get_matchups(matchup_period):
    """Fetch matchup results with opponent info."""
    return query_snowflake("""
        SELECT team_name, team_points, opponent_name,
               opponent_points, result
        FROM int_weekly_matchups
        WHERE matchup_period = %s
    """, (matchup_period,))


def find_tough_luck(scores, matchups):
    """
    Tough Luck: only triggers if the #2 overall scorer lost.
    """
    ranked = sorted(scores, key=lambda x: x['total_points'], reverse=True)
    second_place = ranked[1]

    matchup = next(
        (m for m in matchups if m['team_name'] == second_place['team_name']),
        None
    )
    if matchup and matchup['result'] == 'L':
        return {
            'team': second_place['team_name'],
            'points': second_place['total_points'],
            'opponent': matchup['opponent_name'],
            'opponent_points': matchup['opponent_points'],
        }
    return None


def find_lucky_bastard(scores, matchups):
    """
    Lucky Bastard: only triggers if the #13-of-14 scorer (second lowest) won.
    """
    ranked = sorted(scores, key=lambda x: x['total_points'], reverse=True)
    second_worst = ranked[-2]

    matchup = next(
        (m for m in matchups if m['team_name'] == second_worst['team_name']),
        None
    )
    if matchup and matchup['result'] == 'W':
        return {
            'team': second_worst['team_name'],
            'points': second_worst['total_points'],
            'opponent': matchup['opponent_name'],
            'opponent_points': matchup['opponent_points'],
        }
    return None


def check_fair_and_just(scores, matchups):
    """
    Fair and Just League: did every top-half team (by score) win,
    and every bottom-half team lose?
    
    Uses active matchup count (not team count) as the cutoff,
    which correctly handles odd-team leagues with a bye week.
    """
    ranked = sorted(scores, key=lambda x: x['total_points'], reverse=True)
    num_matchups = len(matchups) // 2  # <-- was: len(ranked) // 2

    for i, team in enumerate(ranked):
        matchup = next(
            (m for m in matchups if m['team_name'] == team['team_name']),
            None
        )
        if not matchup:
            return False
        if i < num_matchups and matchup['result'] != 'W':
            return False
        if i >= num_matchups and matchup['result'] != 'L':
            return False
    return True


def generate_summary(matchup_period, scores, matchups):
    """Build the BBCode-formatted front-page summary."""

    best_overall = scores[0]
    worst_overall = scores[-1]

    by_hitting = sorted(scores, key=lambda x: x['hitting_points'], reverse=True)
    best_hitting = by_hitting[0]
    worst_hitting = by_hitting[-1]

    by_pitching = sorted(scores, key=lambda x: x['pitching_points'], reverse=True)
    best_pitching = by_pitching[0]
    worst_pitching = by_pitching[-1]

    lines = [
        f"[u][b]Matchup #{matchup_period} Recap[/b][/u]",
        f"",
        f"[b]Best Overall[/b]: {best_overall['total_points']:.1f} pts by {best_overall['team_name']}",
        f"[b]Best Hitting[/b]: {best_hitting['hitting_points']:.1f} pts by {best_hitting['team_name']}",
        f"[b]Best Pitching[/b]: {best_pitching['pitching_points']:.1f} pts by {best_pitching['team_name']}",
        f"",
        f"[b]Worst Overall[/b]: {worst_overall['total_points']:.1f} pts by {worst_overall['team_name']}",
        f"[b]Worst Hitting[/b]: {worst_hitting['hitting_points']:.1f} pts by {worst_hitting['team_name']}",
        f"[b]Worst Pitching[/b]: {worst_pitching['pitching_points']:.1f} pts by {worst_pitching['team_name']}",
    ]

    # Tough Luck
    tough_luck = find_tough_luck(scores, matchups)
    if tough_luck:
        lines.extend([
            f"",
            f"[b]Tough Luck[/b]: {tough_luck['team']} scored {tough_luck['points']:.1f} pts, "
            f"second most in the league, but lost to "
            f"{tough_luck['opponent']}'s {tough_luck['opponent_points']:.1f}",
        ])

    # Lucky Bastard
    lucky = find_lucky_bastard(scores, matchups)
    if lucky:
        lines.extend([
            f"",
            f"[b]Lucky Bastard[/b]: {lucky['team']} scored just {lucky['points']:.1f} pts, "
            f"second worst in the league, but beat "
            f"{lucky['opponent']}'s {lucky['opponent_points']:.1f}",
        ])

    # Fair and Just League
    if check_fair_and_just(scores, matchups):
        num_matchups = len(matchups) // 2
        lines.extend([
            f"",
            f"[b]A FAIR AND JUST LEAGUE![/b] The top {num_matchups} scoring teams "
            f"all won this week, and the bottom {num_matchups} all lost.",
        ])

    return "\n".join(lines)

if __name__ == "__main__":
    matchup_period, scores = get_weekly_scores()
    matchups = get_matchups(matchup_period)
    summary = generate_summary(matchup_period, scores, matchups)
    print(summary)