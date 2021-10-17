/* got stuck - l;eft hjere
https://stackoverflow.com/questions/19601948/must-appear-in-the-group-by-clause-or-be-used-in-an-aggregate-function
need to filter odds data to the correct date given */


with schedule_data as (
    select
        *,
        case
            when
                start_time = '' then '8:00'
            /* this was for empty values - im setting a default here bc fk it */
            else start_time
        end as start_time2
    from {{ ref('staging_aws_schedule_table')}}
),

home_team_attributes as (
    select
        staging_seed_team_attributes.team as home_team,
        staging_seed_team_attributes.team_acronym as home_team_acronym,
        staging_seed_team_attributes.previous_season_rank as home_team_prev_rank
    from {{ ref('staging_seed_team_attributes')}}
),

home_team_odds as (
    select
        team_acronym as home_team_acronym,
        (array_agg(moneyline order by date desc))[1] as home_moneyline,
        /* grabs the most recent moneyline odds */
        max(date) as proper_date
    from {{ ref('staging_aws_odds_table')}}
    group by home_team_acronym
),

away_team_odds as (
    select
        team_acronym as away_team_acronym,
        (array_agg(moneyline order by date desc))[1] as away_moneyline,
        /* grabs the most recent moneyline odds */
        max(date) as proper_date
    from {{ ref('staging_aws_odds_table')}}
    group by away_team_acronym
),

away_team_attributes as (
    select
        team as away_team,
        team_acronym as away_team_acronym,
        previous_season_rank as away_team_prev_rank
    from {{ ref('staging_seed_team_attributes')}}
),

final_table as (
    select
        schedule_data.start_time2 as start_time,
        schedule_data.day_name,
        schedule_data.away_team,
        schedule_data.home_team,
        schedule_data.date,
        schedule_data.proper_date,
        home_team_attributes.home_team_acronym,
        home_team_attributes.home_team_prev_rank,
        away_team_attributes.away_team_acronym,
        away_team_attributes.away_team_prev_rank,

        home_team_odds.home_moneyline,

        away_team_odds.away_moneyline,
        (
            away_team_attributes.away_team_prev_rank + home_team_attributes.home_team_prev_rank
        ) / 2 as avg_team_rank
    from schedule_data
    left join home_team_attributes using (home_team)
    left join away_team_attributes using (away_team)
    left join
        home_team_odds on
            home_team_attributes.home_team_acronym =
            home_team_odds.home_team_acronym and schedule_data.proper_date =
            home_team_odds.proper_date
    left join
        away_team_odds on
            away_team_attributes.away_team_acronym =
            away_team_odds.away_team_acronym and schedule_data.proper_date =
            away_team_odds.proper_date
    order by proper_date asc
)

select
    *,
    concat(
        proper_date::text, ' ', start_time::text, ':00'
    )::timestamp as proper_time
from final_table
order by proper_time

/* WIP
,
        CASE WHEN ho.home_moneyline > 0 THEN CONCAT('+', ho.home_moneyline::text)
                ELSE ho.home_moneyline END as home_moneyline2,
        CASE WHEN ao.away_moneyline > 0 THEN CONCAT('+', ao.away_moneyline::text)
                ELSE ao.away_moneyline END as away_moneyline2
                */