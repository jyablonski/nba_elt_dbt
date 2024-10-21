/* to do - last 10 games column+ win streaks */

with team_wins as (
    select distinct
        b.game_date,
        b.outcome,
        a.conference,
        (b.team),
        case
            when outcome = 'W' then 1
            else 0
        end as outcome_int
    from {{ ref('boxscores') }} as b
        left join {{ ref('teams') }} as a on b.team = a.team_acronym
    where season_type = 'Regular Season'

),

active_injuries as (
    select
        team,
        team_active_injuries,
        team_active_protocols
    from {{ ref('team_injury_count_aggs') }}

),

team_counts as (
    select
        team,
        count(*) as games_played,
        sum(outcome_int) as wins,
        (count(*) - sum(outcome_int)) as losses
    from team_wins
    group by team
),

team_attributes as (
    select
        team_acronym as team,
        team as team_full,
        conference
    from {{ ref('teams') }}
),

pre_final as (
    select distinct
        team_attributes.team_full,
        team_attributes.conference,
        team_counts.games_played,
        coalesce(team_counts.wins, 0) as wins,
        coalesce(team_counts.losses, 0) as losses,
        (team_attributes.team) as team,
        coalesce(active_injuries.team_active_injuries, 0) as active_injuries,
        coalesce(active_injuries.team_active_protocols, 0) as active_protocols,
        (team_counts.wins::numeric / games_played::numeric) as win_percentage
    from team_attributes
        left join team_wins on team_attributes.team = team_wins.team
        left join team_counts on team_wins.team = team_counts.team
        left join active_injuries on team_attributes.team_full = active_injuries.team

),

recent_10 as (
    select
        team,
        game_date,
        outcome,
        outcome_int,
        row_number() over (partition by team order by game_date desc) as game_num
    from team_wins
    order by row_number() over (partition by team order by game_date desc)
),

recent_10_wins as (
    select
        team,
        sum(outcome_int) as wins_last_10
    from recent_10
    where game_num <= 10
    group by team
),

recent_10_losses as (
    select
        team,
        game_date,
        outcome,
        case when outcome = 'L' then 1 else 0 end as loss_count
    from recent_10
    where outcome = 'L' and game_num <= 10
    order by game_date desc

),

recent_10_losses_group as (
    select
        team,
        sum(loss_count) as losses_last_10
    from recent_10_losses
    group by team
),

preseason as (
    select
        team_acronym as team,
        championship_odds,
        predicted_wins,
        predicted_losses
    from {{ ref('preseason_odds_data') }}
),

final as (
    select
        *,
        round(win_percentage * 82, 0)::numeric as projected_wins,
        case
            when win_percentage >= 0.5 then 'Above .500'
            else 'Below .500'
        end as team_status,
        82 - round(win_percentage * 82, 0)::numeric as projected_losses
    from pre_final
        left join recent_10_wins using (team)
        left join recent_10_losses_group using (team)
        left join preseason using (team)
)

select
    team,
    team_full,
    conference,
    coalesce(games_played, 0) as games_played,
    coalesce(wins, 0) as wins,
    coalesce(losses, 0) as losses,
    active_injuries,
    active_protocols,
    round(win_percentage, 3)::numeric as win_percentage,
    championship_odds,
    predicted_wins,
    predicted_losses,
    team_status,
    projected_wins,
    projected_losses,
    coalesce(wins_last_10, 0) as wins_last_10,
    coalesce(losses_last_10, 0) as losses_last_10
from final
order by win_percentage desc
/*
select *
from final
*/
