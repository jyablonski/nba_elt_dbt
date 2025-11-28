{{ config(materialized='incremental') }}

with teams as (
    select
        team::text as team,
        team_acronym::text as team_acronym,
        conference,
        primary_color,
        secondary_color,
        third_color,
        previous_season_wins,
        previous_season_rank,
        team_logo,
        created_at,
        modified_at
    from {{ source('bronze', 'internal_team_attributes') }}
    {% if is_incremental() %}
        where modified_at > (select coalesce(max(modified_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

arena_locations as (
    select
        team::text as team_acronym,
        arena,
        latitude as arena_latitude,
        longitude as arena_longitude
    from {{ source('bronze', 'internal_league_arena_locations') }}
)

select
    teams.team,
    teams.team_acronym,
    teams.conference,
    teams.primary_color,
    teams.secondary_color,
    teams.third_color,
    teams.previous_season_wins,
    teams.previous_season_rank,
    teams.team_logo,
    teams.created_at,
    teams.modified_at,
    arena_locations.arena,
    arena_locations.arena_latitude,
    arena_locations.arena_longitude
from teams
    left join arena_locations
        on teams.team_acronym = arena_locations.team_acronym
