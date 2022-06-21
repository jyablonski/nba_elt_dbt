with player_most_recent_date as (
    select
        player,
        max(date)::date as date
    from {{ ref('staging_aws_boxscores_incremental_table') }}
    group by player
),

player_data as (
    select
        player,
        team,
        date
    from {{ ref('staging_aws_boxscores_incremental_table') }}
),

-- this gives the most recent team
player_most_recent_team as (
    select
        player,
        team as most_recent_team
    from player_data
    inner join player_most_recent_date using (player, date)
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
        min(date) as first_appeared_date,
        max(date) as last_appeared_date
    from player_data
    group by player, team

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
        case when team = most_recent_team then TRUE
             else FALSE
             end as is_active_team
    from player_team_dates
    left join player_data_num_teams_agg using (player)
    full outer join player_most_recent_team using (player)
)

select *
from final
where is_active_team = TRUE  -- and player = 'Greg Monroe'