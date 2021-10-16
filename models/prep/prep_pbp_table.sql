/* need to add in game variable from somewhere */
WITH pbp_data as (
SELECT  time_quarter,
        {{dbt_utils.split_part('time_quarter', " ':' ", 1)}}::numeric as minutes,
        {{dbt_utils.split_part('time_quarter', " ':' ", 2)}}::numeric as seconds,
        description_play_visitor,
        away_score, 
        score,
        home_score,
        description_play_home,
        quarter,
        home_team,
        away_team,
        score_away,
        score_home,
        margin_score,
        date,
        CASE WHEN quarter = '1st Quarter' THEN 48
                 WHEN quarter = '2nd Quarter' THEN 36
                 WHEN quarter = '3rd Quarter' THEN 24
                 WHEN quarter = '4th Quarter' Then 12
                 WHEN quarter = '1st OT' THEN 0
                 WHEN quarter = '2nd OT' THEN -5
                 WHEN quarter = '3rd OT' THEN -10
                 WHEN quarter = '4th OT' THEN -15
                 ELSE -20
                 END as quarter_time
FROM {{ ref('staging_aws_pbp_data_table')}}

),

pbp_data2 as (
    SELECT  *,
            ((minutes * 60) + seconds) as seconds_remaining_quarter,
            (720 - ((minutes * 60) + seconds))::numeric as seconds_used_quarter,
            (quarter_time * 60)::numeric as total_time_left_before

                 
    FROM pbp_data
),

pbp_data3 as (
    SELECT  *,
            (total_time_left_before - seconds_used_quarter)::numeric as total_time_left_game

    FROM pbp_data2
),

pbp_data4 as (
    SELECT *,
            round((total_time_left_game / 60), 2)::numeric as time_remaining
    FROM pbp_data3

),

pbp_data5 as (
    SELECT *,
            CASE WHEN quarter = '1st OT' THEN -5*60 + seconds_remaining_quarter
                 WHEN quarter = '2nd OT' THEN -10*60 + seconds_remaining_quarter
                 WHEN quarter = '3rd OT' THEN -15*60 + seconds_remaining_quarter
                 WHEN quarter = '4th OT' THEN -20*60 + seconds_remaining_quarter
                 ELSE total_time_left_game
            END AS time_remaining_adj
    FROM pbp_data4
),

pbp_data6 as (
    SELECT *,
            round((time_remaining_adj/ 60), 2)::numeric as time_remaining_final,
            CASE WHEN score_home > score_away THEN home_team
                 WHEN score_home < score_away THEN away_team
                 ELSE 'TIE'
                 END as leading_team
    FROM pbp_data5
),

pbp_data7 as (
    SELECT *,
            COALESCE(LAG(time_remaining_final, 1) OVER (), 0) AS before_time
    FROM pbp_data6
),

pbp_data8 as (
    SELECT *,
            COALESCE((before_time - time_remaining_final), 0)::numeric as time_difference,
            CASE WHEN description_play_home LIKE description_play_visitor THEN description_play_home
                WHEN description_play_home IS NULL THEN description_play_visitor
                WHEN description_play_visitor IS NULL THEN description_play_home 
                ELSE home_score
                END AS play
    FROM pbp_data7
)

SELECT *
FROM pbp_data8
