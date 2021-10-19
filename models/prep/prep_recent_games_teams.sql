with teams_scores as (
    select
        team,
        date,
        sum(pts) as pts_game
    from {{ ref('staging_aws_boxscores_table')}}
    group by team, date
),

teams_max_score as (
    select
         team,
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
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".team,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".date,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".game_id,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".opponent,
        "jacob_db"."nba_staging"."staging_aws_boxscores_table".outcome,
        sum("jacob_db"."nba_staging"."staging_aws_boxscores_table".pts) as pts_scored
    from {{ ref('staging_aws_boxscores_table')}}
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
        team_pts_scored.team,
        team_logo.team as full_team,
        team_pts_scored.date,
        team_pts_scored.game_id,
        team_pts_scored.outcome,
        team_pts_scored.opponent,
        team_pts_scored.pts_scored,
        opponent_scores.pts_scored_opp,
        teams_max_score.team_avg_score,
        teams_max_score.team_max_score,
        team_logo.team_logo,
        case when pts_scored = team_max_score then 1
                  when
                pts_scored != team_max_score and (
                    pts_scored - team_avg_score
                ) > 10 then 2
                  else 0 end as pts_color,
        (pts_scored - pts_scored_opp)::numeric as mov
    from team_pts_scored
    left join teams_max_score on team_pts_scored.team = teams_max_score.team
    left join team_logo on team_logo.team_acronym = team_pts_scored.team
    left join
        opponent_scores on
            team_pts_scored.opponent = opponent_scores.opponent and team_pts_scored.date = opponent_scores.date
)


select *
from select_final_games
