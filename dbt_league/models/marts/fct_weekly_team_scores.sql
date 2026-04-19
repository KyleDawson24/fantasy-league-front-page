-- fct_weekly_team_scores.sql
-- Roll up daily team scores into weekly matchup-period totals.
-- One row per team per matchup period.
-- This is the table the output script queries.

with daily as (
    select * from {{ ref('int_team_daily_scores') }}
),

weekly as (
    select
        matchup_period,
        team_name,
        team_id,
        sum(total_points)       as total_points,
        sum(hitting_points)     as hitting_points,
        sum(pitching_points)    as pitching_points,
        count(distinct scoring_period) as days_in_period
    from daily
    group by 1, 2, 3
)

select * from weekly