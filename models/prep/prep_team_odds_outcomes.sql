{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

with team_scores as (
    select
        team,
        opponent,
        game_date,
        season_type,
        outcome,
        pts_scored,
        pts_scored_opp,
        mov
    from {{ ref('prep_recent_games_teams') }}
    {% if is_incremental() %}
    where game_date > (select max(game_date) from {{ this }})

    {% endif %}
),

odds_data as (
    select
        team_acronym as team,
        spread,
        moneyline,
        date as game_date
    from {{ ref('fact_odds_data') }}
    {% if is_incremental() %}
    where date > (select max(game_date) from {{ this }})

    {% endif %}
),

combo as (
    select
        {{ dbt_utils.generate_surrogate_key(["dim_teams.team_acronym", "team_scores.game_date"]) }} as id,
        dim_teams.team_acronym as team,
        team_scores.opponent,
        team_scores.game_date,
        team_scores.season_type,
        team_scores.outcome,
        team_scores.pts_scored,
        team_scores.pts_scored_opp,
        team_scores.mov,
        odds_data.spread,
        odds_data.moneyline,
        case
            -- favorite (-spread) covers if they win by more than the spread
            when spread < 0 and mov > abs(spread) then 1

            -- underdog (+spread) covers if they win outright
            when spread > 0 and mov > 0 then 1

            -- underdog (+spread) covers if they lose by less than the spread
            when spread > 0 and mov > -spread then 1

            -- otherwise, the bums did not cover
            else 0
        end as covered_spread,
        current_timestamp as __created_at
    from {{ ref('dim_teams') }}
        inner join team_scores on
            dim_teams.team_acronym = team_scores.team
        inner join odds_data on
            team_scores.team = odds_data.team
            and team_scores.game_date = odds_data.game_date
)

select *
from combo
