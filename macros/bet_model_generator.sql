{% macro bet_model_generator(lower_param, higher_param) %}

  {% set query %}
        select
        proper_date,
        home_team,
        home_moneyline,
        home_team_predicted_win_pct,
        away_team,
        away_moneyline,
        away_team_predicted_win_pct,
        ml_prediction,
        actual_outcome,
        ml_money_col,
        case when ml_money_col > 0 then ml_money_col - 10
            else ml_money_col end as ml_money_col2,
        {{ lower_param }} as bet_parameter_lower_neg_{{lower_param * -1}}
    from jacob_db.ml_models.ml_past_games_odds_analysis
    where (
        (home_moneyline between {{ lower_param }} and {{ higher_param }}
            AND home_team_predicted_win_pct >= 0.55)
        OR (away_moneyline between {{ lower_param }} and {{ higher_param }}
            AND away_team_predicted_win_pct >= 0.55)
        )
    {% endset %}

    {% set results = run_query(query) %}
    {# execute is a Jinja variable that returns True when dbt is in "execute" mode i.e. True when running dbt run but False during dbt compile. #}
    {% if execute %}
    {% set results_list = results.rows %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}
{% endmacro %}
