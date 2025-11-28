-- TRAVEL DISTANCE CALCULATION:
-- Calculates miles traveled between consecutive games for each team
-- Uses Haversine formula to compute great circle distance between arena locations
-- Only includes games up to and including today

with schedule_unpivoted as (
    -- Get one row per team per game with their home arena location
    select
        away_team_acronym as team,
        proper_date as game_date,
        home_teams.arena as opponent_arena,
        home_teams.arena_latitude as opponent_latitude,
        home_teams.arena_longitude as opponent_longitude
    from {{ ref('fact_schedule_data') }}
        inner join {{ ref('dim_teams') }} as home_teams
            on fact_schedule_data.home_team_acronym = home_teams.team_acronym
    where proper_date <= current_date

    union all

    select
        home_team_acronym as team,
        proper_date as game_date,
        home_teams.arena as opponent_arena,
        home_teams.arena_latitude as opponent_latitude,
        home_teams.arena_longitude as opponent_longitude
    from {{ ref('fact_schedule_data') }}
        inner join {{ ref('dim_teams') }} as home_teams
            on fact_schedule_data.home_team_acronym = home_teams.team_acronym
    where proper_date <= current_date
),

schedule_with_previous_location as (
    select
        schedule_unpivoted.*,
        dim_teams.arena as team_home_arena,
        dim_teams.arena_latitude as team_home_latitude,
        dim_teams.arena_longitude as team_home_longitude,
        lag(schedule_unpivoted.opponent_arena) over (partition by schedule_unpivoted.team order by game_date, opponent_arena) as prev_arena,
        lag(schedule_unpivoted.opponent_latitude) over (partition by schedule_unpivoted.team order by game_date, opponent_arena) as prev_latitude,
        lag(schedule_unpivoted.opponent_longitude) over (partition by schedule_unpivoted.team order by game_date, opponent_arena) as prev_longitude
    from schedule_unpivoted
        inner join {{ ref('dim_teams') }} as dim_teams
            on schedule_unpivoted.team = dim_teams.team_acronym
),

travel_legs as (
    select
        team,
        game_date,
        prev_arena,
        opponent_arena,
        round(
            case
                when prev_arena is null
                    then
                        -- Travel from home arena to first game location
                        {{ schedule_haversine_miles('team_home_latitude', 'team_home_longitude', 'opponent_latitude', 'opponent_longitude') }}
                else {{ schedule_haversine_miles('prev_latitude', 'prev_longitude', 'opponent_latitude', 'opponent_longitude') }}
            end
        ) as miles_traveled_to_game
    from schedule_with_previous_location
),

rolling_aggregations as (
    select
        team,
        game_date,
        miles_traveled_to_game,
        -- Cumulative season totals
        round(sum(miles_traveled_to_game) over (
            partition by team
            order by game_date
            rows between unbounded preceding and current row
        )) as total_miles_traveled,
        count(*) over (
            partition by team
            order by game_date
            rows between unbounded preceding and current row
        ) as total_games_played,
        round(avg(miles_traveled_to_game) over (
            partition by team
            order by game_date
            rows between unbounded preceding and current row
        )) as avg_miles_per_game,
        -- Rolling 7-day window
        round(sum(miles_traveled_to_game) over (
            partition by team
            order by game_date
            range between interval '6 days' preceding and current row
        )) as travel_miles_last_7_days,
        count(*) over (
            partition by team
            order by game_date
            range between interval '6 days' preceding and current row
        ) as games_last_7_days,
        -- Rolling 3-day window for cross-country trip flag
        round(sum(miles_traveled_to_game) over (
            partition by team
            order by game_date
            range between interval '2 days' preceding and current row
        )) as travel_miles_last_3_days
    from travel_legs
),

final as (
    select
        team,
        game_date,
        miles_traveled_to_game,
        total_miles_traveled,
        total_games_played,
        avg_miles_per_game,
        travel_miles_last_7_days,
        games_last_7_days,
        travel_miles_last_3_days,
        coalesce(travel_miles_last_3_days > 2000, false) as is_cross_country_trip
    from rolling_aggregations
)

select *
from final
order by
    team,
    game_date
