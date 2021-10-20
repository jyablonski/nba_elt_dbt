/* need to add in game variable from somewhere */
WITH pbp_data AS (
    SELECT
         time_quarter,
        {{dbt_utils.split_part('time_quarter', " ':' ", 1)}}::numeric as minutes,
        {{dbt_utils.split_part('time_quarter', " ':' ", 2)}}::numeric as seconds,
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
        split_part(
            time_quarter,
            ':',
            1
        )

        ::numeric AS minutes,
        split_part(
            time_quarter,
            ':',
            2
        )

        ::numeric AS seconds,
        CASE WHEN quarter = '1st Quarter' THEN 48
                   WHEN quarter = '2nd Quarter' THEN 36
                   WHEN quarter = '3rd Quarter' THEN 24
                   WHEN quarter = '4th Quarter' THEN 12
                   WHEN quarter = '1st OT' THEN 0
                   WHEN quarter = '2nd OT' THEN -5
                   WHEN quarter = '3rd OT' THEN -10
                   WHEN quarter = '4th OT' THEN -15
                   ELSE -20
        END AS quarter_time
    FROM {{ ref('staging_aws_pbp_data_table')}}

),

pbp_data2 AS (
    SELECT
         *,
         ((minutes * 60) + seconds) AS seconds_remaining_quarter,
         (720 - ((minutes * 60) + seconds))::numeric AS seconds_used_quarter,
         (quarter_time * 60)::numeric AS total_time_left_before


    FROM pbp_data
),

pbp_data3 AS (
    SELECT
         *,
         (
             total_time_left_before - seconds_used_quarter
    )::numeric AS total_time_left_game

    FROM pbp_data2
),

pbp_data4 AS (
    SELECT
         *,
         round((total_time_left_game / 60), 2)::numeric AS time_remaining
    FROM pbp_data3

),

pbp_data5 AS (
    SELECT
         *,
         CASE WHEN quarter = '1st OT' THEN -5 * 60 + seconds_remaining_quarter
            WHEN quarter = '2nd OT' THEN -10 * 60 + seconds_remaining_quarter
            WHEN quarter = '3rd OT' THEN -15 * 60 + seconds_remaining_quarter
            WHEN quarter = '4th OT' THEN -20 * 60 + seconds_remaining_quarter
            ELSE total_time_left_game
 END AS time_remaining_adj
    FROM pbp_data4
),

pbp_data6 AS (
    SELECT
         *,
         round((time_remaining_adj / 60), 2)::numeric AS time_remaining_final,
         CASE WHEN score_home > score_away THEN home_team
            WHEN score_home < score_away THEN away_team
            ELSE 'TIE'
        END AS leading_team
    FROM pbp_data5
),

pbp_data7 AS (
    SELECT
         *,
         coalesce(lag(time_remaining_final, 1) OVER (), 0) AS before_time
    FROM pbp_data6
),

pbp_data8 AS (
    SELECT
         *,
         coalesce((before_time - time_remaining_final), 0)::numeric AS time_difference,
         CASE
             WHEN
            description_play_home LIKE description_play_visitor THEN description_play_home
             WHEN description_play_home IS NULL THEN description_play_visitor
             WHEN description_play_visitor IS NULL THEN description_play_home
             ELSE home_score
 END AS play
    FROM pbp_data7
)

SELECT *
FROM pbp_data8
