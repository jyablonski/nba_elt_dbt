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

opponent_logo as (
    select
        team_acronym as opponent,
        team_logo as opp_logo
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
        opponent_logo.opp_logo as opp_logo,
        CASE WHEN pts_scored = team_max_score THEN 1
             when (pts_scored >= team_avg_score + 10) AND (pts_scored != team_max_score) then 2
             when team_avg_score - pts_scored > 10 then 3
             ELSE 0 END AS pts_color,
        (pts_scored - pts_scored_opp)::numeric as mov
    from team_pts_scored b
    left join teams_max_score m on b.team = m.team
    LEFT JOIN team_logo l on l.team_acronym = b.team
    left join opponent_scores o on b.opponent = o.opponent and b.date = o.date
    left join opponent_logo on b.opponent = opponent_logo.opponent
),

final as (
    select *,
            case when abs(mov) BETWEEN 0 and 5 then 'Clutch Game'
             when abs(mov) BETWEEN 6 and 10 then '10 pt Game'
             else 'Blowout Game' end as game_type
    from select_final_games
)


select *
from final