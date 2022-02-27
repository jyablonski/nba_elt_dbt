/* discrepencies bc some teams can both be -110 therefore 'Favored' */

with my_cte as (
    select 
        *,
        case when moneyline < 0 then 'Favored'
        when moneyline > 0 then 'Underdog'
        else 'fuqqq' end as g_type
    from {{ ref('staging_aws_odds_table') }}
),

game_outcomes as (
    select distinct
        team,
        date,
        outcome
    from {{ ref('staging_aws_boxscores_table') }}
),

final as (
    select *
    from my_cte
    left join game_outcomes using (team, date)
    where outcome is not null
),

underdog_win_aggs as (
    select
        outcome,
        g_type,
        count(*) as underdog_wins
    from final
    where outcome = 'W' and g_type = 'Underdog'
    group by 1, 2
),

underdog_losses_aggs as (
    select
        outcome,
        g_type,
        count(*) as underdog_losses
    from final
    where outcome = 'L' and g_type = 'Underdog'
    group by 1, 2
),

favorite_wins_aggs as (
    select
        outcome,
        g_type,
        count(*) as favored_wins
    from final
    where outcome = 'W' and g_type = 'Favored'
    group by 1, 2
),

favorite_losses_aggs as (
    select
        outcome,
        g_type,
        count(*) as favored_losses
    from final
    where outcome = 'L' and g_type = 'Favored'
    group by 1, 2
),


final_two as (
    select 
        final.team,
        final.team_acronym,
        final.date,
        final.outcome,
        final.g_type,
        final.moneyline,
        final.spread,
        case when final.g_type = 'Underdog' then underdog_win_aggs.underdog_wins
        else favorite_wins_aggs.favored_wins end as g_wins,
        case when final.g_type = 'Underdog' then underdog_losses_aggs.underdog_losses
        else favorite_losses_aggs.favored_losses end as g_losses
    from final
    left join underdog_win_aggs using (g_type)
    left join underdog_losses_aggs using (g_type)
    left join favorite_wins_aggs using (g_type)
    left join favorite_losses_aggs using (g_type)
    order by moneyline desc
),

final_three as (
    select 
        *,
        round((g_wins::numeric / (g_wins::numeric + g_losses::numeric)), 3)::numeric as g_win_pct,
        case when moneyline > 0 and outcome = 'W' then moneyline + 100
        else 0 end as money_won_underdogs
    from final_two

)

select *
from final_three