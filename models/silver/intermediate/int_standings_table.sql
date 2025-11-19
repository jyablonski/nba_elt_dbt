with team_wins as (
    select distinct
        fact_boxscores.game_date,
        fact_boxscores.outcome,
        dim_teams.conference,
        fact_boxscores.team,
        case when fact_boxscores.outcome = 'W' then 1 else 0 end as outcome_int
    from {{ ref('fact_boxscores') }}
        left join {{ ref('dim_teams') }}
        on fact_boxscores.team = dim_teams.team_acronym
    where fact_boxscores.season_type = 'Regular Season'
),

active_injuries as (
    select
        team,
        team_active_injuries,
        team_active_protocols
    from {{ ref('int_team_injury_count_aggs') }}
),

team_counts as (
    select
        team_wins.team,
        count(*) as games_played,
        sum(team_wins.outcome_int) as wins,
        count(*) - sum(team_wins.outcome_int) as losses
    from team_wins
    group by team_wins.team
),

team_attributes as (
    select
        dim_teams.team_acronym as team,
        dim_teams.team as team_full,
        dim_teams.conference
    from {{ ref('dim_teams') }}
),

pre_final as (
    select
        team_attributes.team_full,
        team_attributes.conference,
        coalesce(team_counts.games_played, 0) as games_played,
        coalesce(team_counts.wins, 0) as wins,
        coalesce(team_counts.losses, 0) as losses,
        team_attributes.team,
        coalesce(active_injuries.team_active_injuries, 0) as active_injuries,
        coalesce(active_injuries.team_active_protocols, 0) as active_protocols,
        coalesce(team_counts.wins::numeric / nullif(team_counts.games_played, 0)::numeric, 0) as win_percentage
    from team_attributes
        left join team_counts
        on team_attributes.team = team_counts.team
        left join active_injuries
        on team_attributes.team_full = active_injuries.team
),

recent_10 as (
    select
        team_wins.team,
        team_wins.game_date,
        team_wins.outcome,
        team_wins.outcome_int,
        row_number() over (partition by team_wins.team order by team_wins.game_date desc) as game_num
    from team_wins
),

recent_10_wins as (
    select
        recent_10.team,
        sum(recent_10.outcome_int) as wins_last_10
    from recent_10
    where recent_10.game_num <= 10
    group by recent_10.team
),

recent_10_losses_group as (
    select
        recent_10.team,
        sum(case when recent_10.outcome = 'L' then 1 else 0 end) as losses_last_10
    from recent_10
    where recent_10.game_num <= 10
    group by recent_10.team
),

preseason as (
    select
        fact_preseason_odds_data.team_acronym as team,
        fact_preseason_odds_data.championship_odds,
        fact_preseason_odds_data.predicted_wins,
        fact_preseason_odds_data.predicted_losses
    from {{ ref('fact_preseason_odds_data') }}
),

ranked as (
    select
        pre_final.*,
        recent_10_wins.wins_last_10,
        recent_10_losses_group.losses_last_10,
        preseason.championship_odds,
        preseason.predicted_wins,
        preseason.predicted_losses,
        round(pre_final.win_percentage * 82, 0)::numeric as projected_wins,
        82 - round(pre_final.win_percentage * 82, 0)::numeric as projected_losses,
        case
            when pre_final.win_percentage >= 0.5 then 'Above .500'
            else 'Below .500'
        end as team_status,
        rank() over (
            partition by pre_final.conference
            order by pre_final.win_percentage desc
        ) as calculated_rank,
        internal_team_standings_override.season_rank_override,
        coalesce(
            internal_team_standings_override.season_rank_override,
            rank()
                over (
                    partition by pre_final.conference
                    order by pre_final.win_percentage desc
                )
        ) as season_rank
    from pre_final
        left join recent_10_wins
        on pre_final.team = recent_10_wins.team
        left join recent_10_losses_group
        on pre_final.team = recent_10_losses_group.team
        left join preseason
        on pre_final.team = preseason.team
        left join {{ source('bronze', 'internal_team_standings_override') }}
        on pre_final.team = internal_team_standings_override.team
)

select
    ranked.team,
    ranked.team_full,
    ranked.conference,
    ranked.games_played,
    ranked.wins,
    ranked.losses,
    ranked.active_injuries,
    ranked.active_protocols,
    round(ranked.win_percentage, 3)::numeric as win_percentage,
    ranked.championship_odds,
    ranked.predicted_wins,
    ranked.predicted_losses,
    ranked.team_status,
    ranked.projected_wins,
    ranked.projected_losses,
    coalesce(ranked.wins_last_10, 0) as wins_last_10,
    coalesce(ranked.losses_last_10, 0) as losses_last_10,
    ranked.season_rank
from ranked
order by
    ranked.conference asc,
    ranked.season_rank asc
