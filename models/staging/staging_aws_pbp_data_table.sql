/* 12-13-21 - some issue where 2 games have had quarter be null for 1st quarter ?? look into this */

SELECT
    descriptionplayvisitor::text AS description_play_visitor,
    awayscore::text AS away_score,
    score::text AS score,
    homescore::text AS home_score,
    descriptionplayhome::text AS description_play_home,
    coalesce(numberperiod::text, '1st Quarter')::text AS quarter,
    hometeam::text AS home_team,
    awayteam::text AS away_team,
    scoreaway::numeric AS score_away,
    scorehome::numeric AS score_home,
    marginscore::numeric AS margin_score,
    date::date AS date,
    substr(timequarter, 1, length(timequarter) - 2)::text AS time_quarter
FROM {{ source('nba_source', 'aws_pbp_data_source')}}