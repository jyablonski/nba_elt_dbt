with full_odds as (
    select
        *
    from {{ ref('ml_past_games_odds_analysis') }}
),

ml_prediction_count as (
    select
        ml_prediction,
        count(*) as tot_games_location
    from full_odds
    group by 1
),

aggs as (
    select
        ml_accuracy,
        ml_prediction,
        count(*) as num_predictions,
        sum(ml_money_col) as sum_money,
        round((sum(ml_money_col) / count(*)), 2) as avg_money_per_bet
    from full_odds
    group by 1, 2
),

final as (
    select 
        *,
        round(num_predictions::numeric / tot_games_location::numeric, 3) as location_prediction_pct
    from aggs
    left join ml_prediction_count using (ml_prediction)
)

select 
    *
from final

-- old query looking at home/away is_top_player attribute
/*
with full_odds as (
    select
        *
    from {{ ref('ml_past_games_odds_analysis') }}
),

home_analysis as (
    select
        home_is_top_players as top_players_present,
        count(*) as num_predictions,
        sum(ml_money_col),
        round((sum(ml_money_col) / count(*)), 2) as avg_money_per_bet,
        'home' as location
    from full_odds
    group by 1, 5
),

road_analysis as (
    select
        away_is_top_players as top_players_present,
        count(*) as num_predictions,
        sum(ml_money_col),
        round((sum(ml_money_col) / count(*)), 2) as avg_money_per_bet,
        'road' as location
    from full_odds
    group by 1, 5
),

final as (
    select *
    from home_analysis
    union
    select *
    from road_analysis
    order by location, top_players_present
)

select *
from final

*/
