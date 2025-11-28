with player_availability as (
    select
        team,
        available_vorp,
        missing_vorp,
        total_vorp,
        is_player_out
    from {{ ref('int_team_players_vorp') }}
),

aggs as (
    select
        team,
        -- 1. Numerator: Value Missing (Floored at 0)
        sum(case when missing_vorp > 0 then missing_vorp else 0 end) as team_injured_positive_vorp,

        -- 2. Denominator: Total POSITIVE Capacity
        -- We sum ONLY positive contributions from Active and Injured players.
        -- This prevents the denominator from ever being negative.
        sum(case when available_vorp > 0 then available_vorp else 0 end)
        + sum(case when missing_vorp > 0 then missing_vorp else 0 end) as team_total_positive_vorp,

        -- Keep these raw totals for context if needed
        sum(available_vorp) as team_active_net_vorp,
        sum(total_vorp) as team_total_net_vorp,
        sum(is_player_out) as count_players_out
    from player_availability
    group by team
),

final as (
    select
        team,
        team_active_net_vorp as team_active_vorp,
        team_injured_positive_vorp as team_injured_vorp,
        team_total_net_vorp as team_total_vorp,
        count_players_out,

        -- 3. Robust Percentage Calculation
        -- Numerator: Good stuff missing
        -- Denominator: Total Good stuff possible (Active + Injured)
        coalesce(
            round(
                (
                    (team_injured_positive_vorp / nullif(team_total_positive_vorp, 0)) * 100
                )::numeric,
                2
            ),
            0
        ) as pct_vorp_missing
    from aggs
)

select * from final
