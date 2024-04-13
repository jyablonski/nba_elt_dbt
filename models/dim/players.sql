{{ config(materialized='incremental') }}

with players as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        is_rookie,
        yrs_exp,
        headshot,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_player_attributes_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where modified_at > (select max(modified_at) from {{ this }})

    {% endif %}
),

contracts as (
    select
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
            player, 'Kelly Oubre Jr.', 'Kelly Oubre'
        ), 'Kira Lewis Jr.', 'Kira Lewis'),
        'Michael Porter Jr.', 'Michael Porter'), 'Mo Bamba', 'Mohamed Bamba'), 'Jaren Jackson Jr.', 'Jaren Jackson'),
        'Wendell Carter Jr.', 'Wendell Carter'), 'Kenyon Martin Jr.', 'Kenyon Martin'), 'Gary Trent Jr.', 'Gary Trent'),
        'Trey Murphy III', 'Trey Murphy'), 'Larry Nance Jr.', 'Larry Nance'), 'Gary Payton II', 'Gary Payton'),
        'Troy Brown Jr.', 'Troy Brown'), 'Kevin Porter Jr.', 'Kevin Porter'), 'Enes Kanter', 'Enes Freedom') as player,
        team::text as team,
        coalesce(season_salary, 1000000)::numeric as salary
    from {{ source('nba_source', 'aws_contracts_source') }}

),

is_top_players as (
    select
        player,
        team,
        rank
    from {{ source('nba_source', 'team_top_players') }}
)


select
    players.player,
    is_rookie,
    yrs_exp,
    headshot,
    coalesce(salary, 1000000) as salary,
    coalesce(rank, 0) as rank,
    players.created_at,
    players.modified_at
from players
    left join contracts on players.player = contracts.player
    left join is_top_players on players.player = is_top_players.player
