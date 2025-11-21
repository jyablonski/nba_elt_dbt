/* this test looks at all scraped boxscores and compares them to the full schedule table
if there are any games played before today's date that have no records in the boxscores table,
then this test will return an error */

with boxscores_data as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['home_team_df.team', 'away_team_df.team', 'fact_boxscores.game_date']) }} as game_pk,
        home_team_df.team as home_team,
        away_team_df.team as away_team,
        fact_boxscores.game_date
    from {{ ref('fact_boxscores') }}
        inner join {{ ref('dim_teams') }} as home_team_df
            on fact_boxscores.team = home_team_df.team_acronym
        inner join {{ ref('dim_teams') }} as away_team_df
            on fact_boxscores.opponent = away_team_df.team_acronym
    where
        location = 'H'
        -- this is needed so we don't double up on records & games played
)

select *
from {{ ref('int_schedule_table') }}
where
    game_date < current_date
    and game_pk not in (select game_pk from boxscores_data)
