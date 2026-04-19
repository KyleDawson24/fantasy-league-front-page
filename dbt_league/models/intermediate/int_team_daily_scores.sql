-- int_team_daily_scores.sql
-- Aggregate player-level data to team-level daily scores.
-- Splits total points into hitting and pitching categories.
-- Only includes players in active lineup slots (excludes bench and IL).

with players as (
    select * from {{ ref('stg_box_scores') }}
),

active_players as (
    select *
    from players
    where lineup_slot not in ('BE', 'IL')
),

team_daily as (
    select
        season_year,
        scoring_period,
        matchup_period,
        team_name,
        team_id,

        sum(points) as total_points,

        sum(case
            when position in ('SP', 'RP')
            then points else 0
        end) as pitching_points,

        sum(case
            when position not in ('SP', 'RP')
            then points else 0
        end) as hitting_points,

        count(*) as active_player_count

    from active_players
    group by 1, 2, 3, 4, 5
)

select * from team_daily