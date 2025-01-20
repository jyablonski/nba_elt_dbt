with teams_scores as (
    select
        team,
        game_date,
        season_type,
        sum(pts) as pts_game
    from {{ ref('fact_boxscores') }}
    group by
        team,
        game_date,
        season_type
),

-- use regular season max pts and avg pts for the comparisons
teams_max_score as (
    select
        team,
        max(pts_game) as team_max_score,
        avg(pts_game) as team_avg_score
    from teams_scores
    where season_type = 'Regular Season'
    group by team
),

opp_max_score as (
    select
        team as opp,
        max(pts_game) as opp_max_score,
        avg(pts_game) as opp_avg_score
    from teams_scores
    where season_type = 'Regular Season'
    group by team
),


team_logo as (
    select
        team,
        team_acronym,
        team_logo
    from {{ ref('dim_teams') }}
),

opponent_logo as (
    select
        team_acronym as opponent,
        team_logo as opp_logo
    from {{ ref('dim_teams') }}
),

-- this is the table that grabs the most recent games and grabs the pts scored from them
team_pts_scored as (
    select
        team,
        game_date,
        opponent,
        outcome,
        season_type,
        sum(pts) as pts_scored
    from {{ ref('fact_boxscores') }}
    group by
        team,
        game_date,
        opponent,
        outcome,
        season_type
),

opponent_scores as (
    select
        team as opponent,
        game_date,
        pts_scored as pts_scored_opp
    from team_pts_scored

),

select_final_games as (
    select
        b.team,
        l.team as full_team,
        b.game_date,
        b.season_type,
        b.outcome,
        b.opponent,
        b.pts_scored,
        o.pts_scored_opp,
        m.team_max_score,
        s.opp_max_score,
        l.team_logo,
        opponent_logo.opp_logo,
        (pts_scored - pts_scored_opp)::numeric as mov,
        round(m.team_avg_score, 1) as team_avg_score,
        round(s.opp_avg_score, 1) as opp_avg_score,
        case
            when pts_scored = team_max_score then 1
            when (pts_scored >= team_avg_score + 10) and (pts_scored != team_max_score) then 2
            when team_avg_score - pts_scored > 10 then 3
            else 0
        end as pts_color,
        case
            when opp_max_score = pts_scored_opp then 1
            when (pts_scored_opp >= opp_avg_score + 10) and (pts_scored_opp != opp_max_score) then 2
            when (opp_avg_score - pts_scored_opp > 10) then 3
            else 0
        end as opp_pts_color
    from team_pts_scored as b
        left join teams_max_score as m on b.team = m.team
        left join team_logo as l on b.team = l.team_acronym
        left join opponent_scores as o on b.opponent = o.opponent and b.game_date = o.game_date
        left join opponent_logo on b.opponent = opponent_logo.opponent
        left join opp_max_score as s on b.opponent = s.opp
)

select
    *,
    case
        when abs(mov) between 0 and 5 then 'Clutch Game'
        when abs(mov) between 6 and 10 then '10 pt Game'
        when abs(mov) between 11 and 20 then '20 pt Game'
        else 'Blowout Game'
    end as game_type
from select_final_games
order by game_date
