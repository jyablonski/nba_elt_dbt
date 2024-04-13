{{ config(materialized='incremental') }}


-- fucking annoying bc i cant reference new select column aliases to create new cols in postgres
with pbp_raw as (
    select
        descriptionplayvisitor::text as description_play_visitor,
        awayscore::text as away_score,
        score::text as score,
        homescore::text as home_score,
        descriptionplayhome::text as description_play_home,
        coalesce(numberperiod::text, '1st Quarter')::text as quarter,
        hometeam::text as home_team,
        awayteam::text as away_team,
        scoreaway::numeric as score_away,
        scorehome::numeric as score_home,
        marginscore::numeric as margin_score,
        date::date as game_date,
        substr(timequarter, 1, length(timequarter) - 2)::text as time_quarter,
        {{ split_part('substr(timequarter, 1, length(timequarter) - 2)::text', " ':' ", 1) }}::numeric as minutes,
        {{ split_part('substr(timequarter, 1, length(timequarter) - 2)::text', " ':' ", 2) }}::numeric as seconds,
        {{ generate_season_type('date') }}::text as season_type,
        case
            when coalesce(numberperiod::text, '1st Quarter')::text = '1st Quarter' then 48
            when coalesce(numberperiod::text, '1st Quarter')::text = '2nd Quarter' then 36
            when coalesce(numberperiod::text, '1st Quarter')::text = '3rd Quarter' then 24
            when coalesce(numberperiod::text, '1st Quarter')::text = '4th Quarter' then 12
            when coalesce(numberperiod::text, '1st Quarter')::text = '1st OT' then 0
            when coalesce(numberperiod::text, '1st Quarter')::text = '2nd OT' then -5
            when coalesce(numberperiod::text, '1st Quarter')::text = '3rd OT' then -10
            when coalesce(numberperiod::text, '1st Quarter')::text = '4th OT' then -15
            else -20
        end as quarter_time,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_pbp_data_source') }}
    where
        substr(timequarter, 1, length(timequarter) - 2)::text like '%:%' -- needed in case bbref fucks up again and includes faulty time values
        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
            and date > (select max(game_date) from {{ this }})

        {% endif %}

),

time_remaining_calcs as (
    select
        description_play_visitor,
        game_date,
        quarter,
        time_quarter,
        (720 - ((minutes * 60) + seconds))::numeric as seconds_used_quarter,
        (quarter_time * 60)::numeric as total_time_left_before,
        ((minutes * 60) + seconds) as seconds_remaining_quarter,
        ((quarter_time * 60)::numeric - (720 - ((minutes * 60) + seconds))::numeric)::numeric as total_time_left_game,
        round((((quarter_time * 60)::numeric - (720 - ((minutes * 60) + seconds))::numeric)::numeric / 60), 2)::numeric as time_remaining,
        case
            when quarter = '1st OT' then -5 * 60 + ((minutes * 60) + seconds)
            when quarter = '2nd OT' then -10 * 60 + ((minutes * 60) + seconds)
            when quarter = '3rd OT' then -15 * 60 + ((minutes * 60) + seconds)
            when quarter = '4th OT' then -20 * 60 + ((minutes * 60) + seconds)
            else ((quarter_time * 60)::numeric - (720 - ((minutes * 60) + seconds))::numeric)::numeric
        end as time_remaining_adj
    from pbp_raw
),

pbp_adjusted as (
    select distinct
        pbp_raw.*,
        round((time_remaining_adj / 60), 2)::numeric as time_remaining_final,
        coalesce(lag(round((time_remaining_adj / 60), 2)::numeric, 1) over (), 0) as before_time,
        case
            when score_home > score_away then home_team
            when score_home < score_away then away_team
            else 'TIE'
        end as leading_team,
        case
            when description_play_home like pbp_raw.description_play_visitor then description_play_home
            when description_play_home is null then pbp_raw.description_play_visitor
            when pbp_raw.description_play_visitor is null then description_play_home
            else home_score
        end as play,
        coalesce(
            (
                coalesce(lag(round((time_remaining_adj / 60), 2)::numeric, 1) over (), 0)
                - round((time_remaining_adj / 60), 2)::numeric
            ), 0
        )::numeric as time_difference,
        home_team_attributes.team as home_team_full,
        home_team_attributes.primary_color as home_primary_color,
        away_team_attributes.team as away_team_full,
        away_team_attributes.primary_color as away_primary_color,
        concat(home_team_attributes.team, ' Vs. ', away_team_attributes.team) as game_description,
        concat('<span style=''color:', away_team_attributes.primary_color, ''';>', away_team_attributes.team, '</span>') as away_fill,
        concat('<span style=''color:', home_team_attributes.primary_color, ''';>', home_team_attributes.team, '</span>') as home_fill,
        case
            when away_score is null then home_team_attributes.primary_color
            when home_score is null then away_team_attributes.primary_color
            else '#808080'
        end as scoring_team_color,
        case
            when away_score is null then home_team
            when home_score is null then away_team
            else 'TIE'
        end as scoring_team
    from pbp_raw
        inner join time_remaining_calcs
            on
                pbp_raw.description_play_visitor = time_remaining_calcs.description_play_visitor
                and pbp_raw.game_date = time_remaining_calcs.game_date
                and pbp_raw.quarter = time_remaining_calcs.quarter
                and pbp_raw.time_quarter = time_remaining_calcs.time_quarter
        left join {{ source('nba_source', 'aws_team_attributes_source') }} as home_team_attributes
            on pbp_raw.home_team = home_team_attributes.team_acronym
        left join {{ source('nba_source', 'aws_team_attributes_source') }} as away_team_attributes
            on pbp_raw.away_team = away_team_attributes.team_acronym
),

min_max_aggs as (
    select
        game_description,
        game_date,
        min(time_remaining_final) as time_remaining_final,
        max(margin_score) as max_home_lead,
        min(margin_score) as max_away_lead
    from pbp_adjusted
    group by
        game_description,
        game_date
),

lead_measures as (
    select distinct
        leading_team as winning_team,
        max_home_lead,
        max_away_lead,
        pbp_adjusted.game_description,
        pbp_adjusted.game_date,
        case
            when home_team = leading_team then away_team
            else home_team
        end as losing_team
    from pbp_adjusted
        inner join min_max_aggs
            on
                pbp_adjusted.game_description = min_max_aggs.game_description
                and pbp_adjusted.game_date = min_max_aggs.game_date
                and pbp_adjusted.time_remaining_final = min_max_aggs.time_remaining_final
    where leading_team != 'TIE' -- this is incase the game ends w/ free throws at 0.0 like hou vs sac on 2023-02-08 where the lead flips from 1 team to tie to the other team

)

select distinct
    pbp_adjusted.game_date,
    season_type,
    home_team_full,
    home_team,
    away_team_full,
    away_team,
    time_quarter,
    quarter,
    time_remaining_final,
    play,
    away_score,
    score,
    home_score,
    score_away,
    score_home,
    margin_score,
    pbp_adjusted.leading_team,
    home_primary_color,
    away_primary_color,
    pbp_adjusted.game_description,
    away_fill,
    home_fill,
    scoring_team_color,
    scoring_team,
    winning_team,
    losing_team,
    lead_measures.max_home_lead,
    lead_measures.max_away_lead,
    case
        when (scoring_team = pbp_adjusted.leading_team) and (pbp_adjusted.leading_team != 'TIE') then 'Leading'
        when (scoring_team != pbp_adjusted.leading_team) and (pbp_adjusted.leading_team != 'TIE') then 'Trailing'
        else 'TIE'
    end as leading_team_text
from pbp_adjusted
    inner join lead_measures
        on
            pbp_adjusted.game_description = lead_measures.game_description
            and pbp_adjusted.game_date = lead_measures.game_date
