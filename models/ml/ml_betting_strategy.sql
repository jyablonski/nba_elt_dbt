{% set bet_parameter = 15 %}
{% set bet_amounts = range(10, 26) %}

-- TO DO: analyze adjusting the away and home team predicted win pct instead
-- create a case when var to group the predicted win pcts by bins of 5 or something and then calculate betting profit in those bins

with schedule_wins as (
    select 
        a.team as home_team,
        s.date as proper_date,
        s.outcome as outcome
    from {{ ref('prep_schedule_analysis') }} as s
    left join {{ ref('staging_seed_team_attributes') }} as a on a.team_acronym = s.team
    where location = 'H'
),


my_cte as (
    select 
        *,
        case when home_team_predicted_win_pct >= 0.5 then 'Home Win'
            else 'Road Win' end as ml_prediction,
        case when outcome = 'W' then 'Home Win' 
            else 'Road Win' end as actual_outcome
    from {{ source('ml_models', 'tonights_games_ml') }}
    left join schedule_wins using (home_team, proper_date)
    where proper_date < date({{ dbt_utils.current_timestamp() }} - INTERVAL '6 hour')
),

-- the data points actually broken down
-- ml is correct when ml_accuracy = 1
game_predictions as (
    select distinct *,
        case when ml_prediction = actual_outcome then 1 else 0 end as ml_accuracy
    from my_cte
),

home_odds as (
    select
        a.team as home_team,
        date as proper_date,
        moneyline as home_moneyline
    from {{ ref('staging_aws_odds_table') }}
    left join {{ ref('staging_seed_team_attributes') }} a using (team_acronym)
),

away_odds as (
    select
        a.team as away_team,
        date as proper_date,
        moneyline as away_moneyline
    from {{ ref('staging_aws_odds_table') }}
    left join {{ ref('staging_seed_team_attributes') }} a using (team_acronym)
),
-- this shows the actual game outcomes that should be bet on
game_outcomes as (
    select
        away_team,
        home_team,
        proper_date,
        outcome,
        home_team_predicted_win_pct,
        away_team_predicted_win_pct,
        ml_prediction,
        actual_outcome,
        ml_accuracy,
        home_moneyline,
        away_moneyline,
        {% for bet_amount in bet_amounts %}
        case when ml_accuracy = 1 and ml_prediction = 'Home Win' and home_moneyline < 0
            then round('{{ bet_amount }}' * (-100 / home_moneyline), 2)
            when ml_accuracy = 1 and ml_prediction = 'Home Win' and home_moneyline > 0
                then round('{{ bet_amount }}' * (home_moneyline / 100), 2)
            when ml_accuracy = 1 and ml_prediction = 'Road Win' and away_moneyline < 0
                then round('{{ bet_amount }}' * (-100 / away_moneyline), 2)
            when ml_accuracy = 1 and ml_prediction = 'Road Win' and away_moneyline > 0
                then round('{{ bet_amount }}' * (away_moneyline / 100), 2)
            when ml_accuracy = 0 then -1 * '{{ bet_amount }}'
            else -10000  -- im testing to make sure it never hits -10000 - if it does then there's an error
            end as bet_{{bet_amount}}
        {% if not loop.last %},{% endif %} -- you're looping together a million different case whens, so you need commas for that.
        {% endfor %}
    from game_predictions
    left join home_odds using (home_team, proper_date)
    left join away_odds using (away_team, proper_date)
    where (away_team_predicted_win_pct >= 0.55 AND away_team_predicted_win_pct <= 0.75)
         OR
          (home_team_predicted_win_pct >= 0.65 AND home_team_predicted_win_pct <= 0.67)
        AND proper_date < '2022-04-11' -- can choose to include playoffs or not - i find betting to be worse odds than during reg season
    order by proper_date desc
),

final_aggs as (
    select 
        ml_prediction,
        ml_accuracy,
        count(*) as games_bet,
        {% for bet_amount in bet_amounts %}
        sum(bet_{{ bet_amount }}) as tot_profit_{{ bet_amount }}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from game_outcomes
    group by 
        ml_accuracy,
        ml_prediction
),


profit_aggs as (
    select 
        {% for bet_amount in bet_amounts %}
        sum(tot_profit_{{ bet_amount }}) as sum_tot_profit_{{ bet_amount }}
        {% if not loop.last %},{% endif %}
        {% endfor %},
        sum(games_bet) as tot_games_bet
    from final_aggs
),

unnest_aggs as (
    select 
        tot_games_bet,
        unnest(array[
        {% for bet_amount in bet_amounts %}
        {{ bet_amount }} 
        {% if not loop.last %},{% endif %}
        {% endfor %}
        ]) AS bet_amount,
        unnest(array[
        {% for bet_amount in bet_amounts %}
        sum_tot_profit_{{ bet_amount }}
        {% if not loop.last %},{% endif %}
        {% endfor %}
        ]) AS tot_bets_profit
    from profit_aggs   
),

final_metrics as (
    select
        tot_games_bet,
        bet_amount,
        tot_bets_profit,
        round(tot_bets_profit / tot_games_bet, 2) as tot_profit_per_bet
    from unnest_aggs
)

select *
from final_metrics