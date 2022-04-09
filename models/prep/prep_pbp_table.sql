/* need to add in game variable from somewhere */
with pbp_data as (
select  time_quarter,
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
        season_type,
        case when quarter = '1st Quarter' then 48
                 when quarter = '2nd Quarter' then 36
                 when quarter = '3rd Quarter' then 24
                 when quarter = '4th Quarter' then 12
                 when quarter = '1st OT' then 0
                 when quarter = '2nd OT' then -5
                 when quarter = '3rd OT' then -10
                 when quarter = '4th OT' then -15
                 else -20
                 end as quarter_time
from {{ ref('staging_aws_pbp_data_table')}}
where time_quarter like '%:%' -- needed in case bbref fucks up again and includes faulty time values

),

pbp_data2 as (
    select  *,
            ((minutes * 60) + seconds) as seconds_remaining_quarter,
            (720 - ((minutes * 60) + seconds))::numeric as seconds_used_quarter,
            (quarter_time * 60)::numeric as total_time_left_before

                 
    from pbp_data
),

pbp_data3 as (
    select  *,
            (total_time_left_before - seconds_used_quarter)::numeric as total_time_left_game

    from pbp_data2
),

pbp_data4 as (
    select *,
            round((total_time_left_game / 60), 2)::numeric as time_remaining
    from pbp_data3

),

pbp_data5 as (
    select *,
            case when quarter = '1st OT' then -5*60 + seconds_remaining_quarter
                 when quarter = '2nd OT' then -10*60 + seconds_remaining_quarter
                 when quarter = '3rd OT' then -15*60 + seconds_remaining_quarter
                 when quarter = '4th OT' then -20*60 + seconds_remaining_quarter
                 else total_time_left_game
            end as time_remaining_adj
    from pbp_data4
),

pbp_data6 as (
    select *,
            round((time_remaining_adj/ 60), 2)::numeric as time_remaining_final,
            case when score_home > score_away then home_team
                 when score_home < score_away then away_team
                 else 'TIE'
                 end as leading_team
    from pbp_data5
),

pbp_data7 as (
    select *,
            COALESCE(LAG(time_remaining_final, 1) over (), 0) as before_time
    from pbp_data6
),

game_ids as (
    select 
        distinct team as home_team,
        date,
        game_id
    from {{ ref('staging_aws_boxscores_table')}}
),

home_vars as (
    select
        team as home_team_full,
        team_acronym as home_team,
        primary_color as home_primary_color
    from {{ ref('staging_seed_team_attributes')}}

),

away_vars as (
    select
        team as away_team_full,
        team_acronym as away_team,
        primary_color as away_primary_color
    from {{ ref('staging_seed_team_attributes')}}

),

pbp_data8 as (
    select 
        pbp_data7.home_team,
        date,
        season_type,
        time_quarter,
        minutes,
        seconds,
        description_play_visitor,
        away_score,
        score,
        home_score,
        description_play_home,
        quarter,
        pbp_data7.away_team,
        score_away,
        score_home,
        margin_score,
        quarter_time,
        seconds_remaining_quarter,
        seconds_used_quarter,
        total_time_left_before,
        total_time_left_game,
        time_remaining,
        time_remaining_adj,
        time_remaining_final,
        leading_team,
        before_time,
        game_id,
        home_team_full,
        home_primary_color,
        away_team_full,
        away_primary_color,
        COALESCE((before_time - time_remaining_final), 0)::numeric as time_difference,
        case when description_play_home like description_play_visitor then description_play_home
                when description_play_home is null then description_play_visitor
                when description_play_visitor is null then description_play_home 
                else home_score
                end as play,
    CONCAT(home_team_full, ' Vs. ', away_team_full) as game_description
    from pbp_data7
    left join game_ids using (home_team, date)
    left join home_vars on home_vars.home_team = pbp_data7.home_team
    left join away_vars on away_vars.away_team = pbp_data7.away_team

),

final as (
    select *,
        CONCAT('<span style=''color:', away_primary_color, ''';>', away_team_full, '</span>') as away_fill,
        CONCAT('<span style=''color:', home_primary_color, ''';>', home_team_full, '</span>') as home_fill,
        case when away_score is null then home_primary_color
         when home_score is null then away_primary_color
         else '#808080' end as scoring_team_color,
        case when away_score is null then home_team
         when home_score is null then away_team
         else 'TIE' end as scoring_team
    from pbp_data8
),

winning_team as (
    select game_description, date, MIN(time_remaining_final) as time_remaining_final,
    max(margin_score) as max_home_lead,
    min(margin_score) as max_away_lead
    from final
    group by 1, 2
),

winning_team2 as (
    select leading_team as winning_team,
    case when home_team = leading_team then away_team
    else home_team end as losing_team,
    max_home_lead,
    max_away_lead,
     game_description,
     date
    from final
    inner join winning_team using (game_description, date, time_remaining_final)
)

/* have to throw in distinct here otherwise rows will get doubled */
select 
    distinct *
from final
left join winning_team2 using (game_description, date)


