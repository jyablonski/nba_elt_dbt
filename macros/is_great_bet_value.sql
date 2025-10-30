-- these thresholds are manually curated and refreshed every once & inawhile
-- using the views in the ml folder to analyze & identify where the right 
-- profitability breakpoints are
{% macro is_great_bet_value(moneyline_column, predicted_win_pct_column) %}
    case
        when
            (({{ moneyline_column }} >= -130 or {{ moneyline_column }} >= 200) 
             and {{ predicted_win_pct_column }} >= 0.55)
            or ({{ moneyline_column }} >= 170 and {{ predicted_win_pct_column }} >= 0.50) 
        then 1
        else 0
    end
{% endmacro %}