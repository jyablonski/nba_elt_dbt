with player_most_recent_date as (
    select
        player,
        max(game_date)::date as game_date
    from {{ ref('boxscores') }}
    group by player
),

player_data as (
    select
        player,
        team,
        game_date
    from {{ ref('boxscores') }}
),

-- this gives the most recent team
player_most_recent_team as (
    select
        player,
        team as most_recent_team
    from player_data
        inner join player_most_recent_date using (player, game_date)
),

-- this and the _agg cte give the NUMBER of teams the player played on this year
player_data_num_teams as (
    select distinct
        player,
        team
    from player_data
),

player_data_num_teams_agg as (
    select
        player,
        count(*) as num_teams_total
    from player_data_num_teams
    group by player
),

-- this cte grabs the first and last appeared date for each team the player played on
player_team_dates as (
    select
        player,
        team,
        min(game_date) as first_appeared_date,
        max(game_date) as last_appeared_date
    from player_data
    group by 
        player,
        team

),

-- this includes a running list along with first/last dates a player appeared for a team
-- 2022-06-20 UPDATE: im not currently using this but i might for next season.
final as (
    select
        player,
        team,
        num_teams_total,
        first_appeared_date,
        last_appeared_date,
        most_recent_team,
        coalesce(team = most_recent_team, false) as is_active_team
    from player_team_dates
        left join player_data_num_teams_agg using (player)
        full outer join player_most_recent_team using (player)
)

select *
from final
where is_active_team = true  -- and player = 'Greg Monroe'
