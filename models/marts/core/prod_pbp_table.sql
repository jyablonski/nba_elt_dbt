with pbp_table as (
    SELECT *
    FROM {{ ref('prep_pbp_table')}}
),

home_vars as (
    SELECT  team as home_team_full,
            team_acronym home_team,
            primary_color as home_primary_color
    FROM {{ ref('staging_seed_team_attributes')}}

),

away_vars as (
    SELECT  team as away_team_full,
            team_acronym away_team,
            primary_color as away_primary_color
    FROM {{ ref('staging_seed_team_attributes')}}

),

final as (
    SELECT  p.time_quarter,
            p.play,
            p.time_remaining_final,
            p.quarter,
            p.away_score,
            p.score,
            p.home_score,
            p.home_team,
            p.away_team,
            p.score_away,
            p.score_home,
            p.margin_score,
            p.date,
            p.leading_team, 
            h.home_team_full,
            h.home_primary_color,
            a.away_team_full,
            a.away_primary_color,
            CONCAT(home_team_full, ' Vs. ', away_team_full) as game_description,
            CONCAT('<span style=''color:', away_primary_color,''';>', away_team_full, '</span>') as away_fill
    FROM pbp_table p
    LEFT JOIN home_vars h on h.home_team = p.home_team
    LEFT JOIN away_vars a on a.away_team = p.away_team
)


SELECT * 
FROM final