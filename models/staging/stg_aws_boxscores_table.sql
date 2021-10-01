with season_avg_stats as (
    SELECT 
     player::text,
     fga::numeric,
     fta::numeric,
     pts::numeric,
     {{ generate_ts_percent('pts', 'fga', 'fta::numeric') }} as ts_percent
    FROM {{ source('nba_source', 'aws_boxscores_table')}}
    WHERE player IS NOT NULL
),

/*      pts / (2 * (fga + (fta::numeric * 0.44))) as hm */
game_stats as (
    SELECT *
    FROM {{ source('nba_source', 'aws_boxscores_table')}}
    WHERE player IS NOT NULL

),

final_aws_boxscores as (
    SELECT *
    from season_avg_stats

)

SELECT * FROM final_aws_boxscores