with teams_scores as (
    select
        team,
        date,
        sum(pts) as pts_game
    from {{ ref('staging_aws_boxscores_table')}}
    group by team, date
),

teams_max_score as (
    select  team,
            max(pts_game) as team_max_score,
            avg(pts_game) as team_avg_score
    from teams_scores
    group by team
),

team_logo as (
    select
        team,
        team_acronym,
        team_logo
    from {{ ref('staging_seed_team_attributes')}}
),

final_table as (
    select
        *
    from teams_scores
),

team_pts_scored as (
    select 
        b.team,
        b.date,
        b.game_id,
        b.opponent,
        b.outcome,
        sum(b.pts) as pts_scored
    from {{ ref('staging_aws_boxscores_table')}} b
    group by 1, 2, 3, 4, 5
),

opponent_scores as (
    select 
        team as opponent,
        date,
        pts_scored as pts_scored_opp
    from team_pts_scored

),

select_final_games as (
    select
        b.team,
        l.team as full_team,
        b.date,
        b.game_id,
        b.outcome,
        b.opponent,
        b.pts_scored,
        o.pts_scored_opp,
        m.team_avg_score,
        m.team_max_score,
        l.team_logo,
        CASE WHEN pts_scored = team_max_score THEN 1
             WHEN pts_scored != team_max_score AND (pts_scored - team_avg_score) > 10 THEN 2
             ELSE 0 END AS pts_color,
        (pts_scored - pts_scored_opp)::numeric as mov
    from team_pts_scored b
    left join teams_max_score m on b.team = m.team
    LEFT JOIN team_logo l on l.team_acronym = b.team
    left join opponent_scores o on b.opponent = o.opponent and b.date = o.date
)


select *
from select_final_games