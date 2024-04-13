/* got stuck - l;eft hjere
https://stackoverflow.com/questions/19601948/must-appear-in-the-group-by-clause-or-be-used-in-an-aggregate-function
need to filter odds data to the correct date given */

-- 2022-04-13 update - there needs to be updates in the future to fix what i'm doing here
-- i'm like only putting odds data on today's games and everything else is null and filtering on that - that's dumb as fuq.
-- just leave all the odds data on the game records and then filter on current date or something.

with schedule_data as (
    select
        *,
        case
            when
                start_time = '' then '8:00'
            /* this was for empty values - im setting a default here bc fk it */
            else start_time
        end as start_time2
    from {{ ref('schedule_data') }}
),

home_team_attributes as (
    select
        team as home_team,
        team_acronym as home_team_acronym
    from {{ ref('teams') }}
),

home_team_rank as (
    select
        team_full as home_team,
        row_number() over () as home_team_rank
    from {{ ref('prep_standings_table') }}

),

home_team_odds as (
    select
        team_acronym as home_team_acronym,
        moneyline as home_moneyline,
        date as proper_date
    from {{ ref('odds_data') }}
),


away_team_attributes as (
    select
        team as away_team,
        team_acronym as away_team_acronym
    from {{ ref('teams') }}
),

away_team_odds as (
    select
        team_acronym as away_team_acronym,
        moneyline as away_moneyline,
        date as proper_date
    from {{ ref('odds_data') }}
),

away_team_rank as (
    select
        team_full as away_team,
        row_number() over () as away_team_rank
    from {{ ref('prep_standings_table') }}

),

home_days_rest as (
    select
        team as home_team,
        date as proper_date,
        days_rest as home_days_rest
    from {{ ref('prep_team_days_rest') }}
),

away_days_rest as (
    select
        team as away_team,
        date as proper_date,
        days_rest as away_days_rest
    from {{ ref('prep_team_days_rest') }}
),

final_table as (
    select
        schedule_data.start_time2 as start_time,
        rtrim(schedule_data.day_name) as day_name,
        schedule_data.away_team,
        schedule_data.home_team,
        schedule_data.date,
        schedule_data.proper_date as game_date,
        home_team_attributes.home_team_acronym,
        home_team_rank.home_team_rank,
        away_team_attributes.away_team_acronym,
        away_team_rank.away_team_rank,
        home_days_rest.home_days_rest,
        away_days_rest.away_days_rest,
        home_team_odds.home_moneyline,
        away_team_odds.away_moneyline,
        (
            away_team_rank.away_team_rank + home_team_rank.home_team_rank
        ) / 2 as avg_team_rank,
        {{ dbt_utils.generate_surrogate_key(['schedule_data.home_team', 'schedule_data.away_team', 'schedule_data.proper_date']) }} as game_pk,
        cast(concat(
            cast(schedule_data.proper_date as text), ' ', cast(start_time as text), ':00'
        ) as timestamp) as game_ts,
        {{ generate_season_type('schedule_data.proper_date') }}::text as season_type
    from schedule_data
        left join home_team_attributes using (home_team)
        left join away_team_attributes using (away_team)
        left join home_team_rank using (home_team)
        left join away_team_rank using (away_team)
        left join
            home_team_odds
            on
                home_team_attributes.home_team_acronym = home_team_odds.home_team_acronym
                and schedule_data.proper_date = home_team_odds.proper_date
        left join
            away_team_odds
            on
                away_team_attributes.away_team_acronym = away_team_odds.away_team_acronym
                and schedule_data.proper_date = away_team_odds.proper_date
        left join home_days_rest
            on
                schedule_data.home_team = home_days_rest.home_team
                and schedule_data.proper_date = home_days_rest.proper_date
        left join away_days_rest
            on
                schedule_data.away_team = away_days_rest.away_team
                and schedule_data.proper_date = away_days_rest.proper_date
    order by schedule_data.proper_date asc
)

select *
from final_table
