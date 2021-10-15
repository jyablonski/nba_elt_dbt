SELECT  substr(timequarter, 1, length(timequarter) - 2)::text as time_quarter,
        descriptionplayvisitor::text as description_play_visitor,
        awayscore::text as away_score, 
        score::text as score,
        homescore::text as home_score,
        descriptionplayhome::text as description_play_home,
        numberperiod::text as quarter,
        hometeam::text as home_team,
        awayteam::text as away_team,
        scoreaway::numeric as score_away,
        scorehome::numeric as score_home,
        marginscore::numeric as margin_score,
        date::date as date
FROM {{ source('nba_source', 'aws_pbp_data_source')}}