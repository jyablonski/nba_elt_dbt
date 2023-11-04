# Apuntes
`pip install sqlfluff`
`pip install sqlfluff-templater-dbt`


```
# L016 - line too long
# L031 - avoid table aliasessss
# L034 - use wildcards first in select like select *, a * 2, b * 4 etc
```

`sqlfluff lint models/marts/bans.sql`
`sqlfluff fix models/marts/bans.sql`

stackoverflow how to fix 


[sqlfluff default config](https://docs.sqlfluff.com/en/stable/configuration.html#default-configuration)


```
    entry: bash -c 'env DBT_PRAC_KEY=hooks'
    additional_dependencies: [
    'sqlfluff==2.3.0',
    'sqlfluff-templater-dbt==2.3.0',
    'dbt-core==1.5.0',
    'dbt-postgres==1.5.0'
    ]
```

https://www.datafold.com/blog/accelerating-dbt-core-ci-cd-with-github-actions-a-step-by-step-guide
https://stackoverflow.com/questions/75286648/multiple-profiles-in-one-profiles-yml-is-possible
https://discourse.getdbt.com/t/how-we-sped-up-our-ci-runs-by-10x-using-slim-ci/2603
https://github.com/RealSelf/dbt-source/blob/development/sample.profiles.yml

`dbt build --select config.materialized:view --target prod`

## Profiles 
`profiles.yml`
- Works with `dbt_project.yml` to define a default target


```
CREATE TABLE IF NOT EXISTS ml_tonights_games_prod (
	home_team text NULL,
	away_team text NULL,
	home_moneyline numeric NULL,
	away_moneyline numeric NULL,
	proper_date date NULL,
	home_team_rank int8 NULL,
    home_days_rest int4 NULL,
	home_team_avg_pts_scored numeric NULL,
	home_team_avg_pts_scored_opp numeric NULL,
	home_team_win_pct numeric NULL,
	home_team_win_pct_last10 numeric NULL,
	home_is_top_players numeric NULL,
	away_team_rank int8 NULL,
    away_days_rest int4 NULL,
	away_team_avg_pts_scored numeric NULL,
	away_team_avg_pts_scored_opp numeric NULL,
	away_team_win_pct numeric NULL,
	away_team_win_pct_last10 numeric NULL,
	away_is_top_players numeric NULL,
	outcome int4 NULL
);

```

``` sh
dbt debug --profiles-dir profiles --profile dbt_ci
dbt build --profiles-dir profiles --profile dbt_ci

poetry run dbt build --target dev --profiles-dir profiles/ --profile dbt_ci --select state:modified+ --state ./

```

```
> git -c user.useConfigOnly=true commit --quiet --allow-empty-message --file -
sqlfluff-lint............................................................Failed
- hook id: sqlfluff-lint
- exit code: 1

=== [dbt templater] Sorting Nodes...
=== [dbt templater] Compiling dbt project...
=== [dbt templater] Project Compiled.
== [models/ml/ml_past_games_analysis_total_aggs.sql] FAIL
L:  61 | P:  13 | LT02 | Expected line break and indent of 12 spaces before
                       | 'when'. [layout.indent]
L:  61 | P:  18 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  62 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  62 | P:  57 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
== [models/ml/ml_past_games_analysis_daily.sql] FAIL
L:  51 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  54 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  60 | P:   5 | LT09 | Select targets should be on a new line unless there is
                       | only one select target. [layout.select_targets]
L:  60 | P:  20 | LT02 | Expected line break and indent of 8 spaces before '*'.
                       | [layout.indent]
L: 101 | P:  25 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 102 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L: 102 | P:  34 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 103 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L: 103 | P:  36 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 104 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
L: 110 | P:  62 |  PRS | Line 114, Position 62: Found unparsable section: " ROWS
                       | BETWEEN '6' PRECEDING AND CURRENT ..."
L: 112 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
== [models/prep/prep_player_stats_rolling_avg.sql] FAIL
L:  26 | P:  50 |  PRS | Line 26, Position 50: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  27 | P:  62 |  PRS | Line 27, Position 62: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  28 | P:  61 |  PRS | Line 28, Position 61: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  29 | P:  57 |  PRS | Line 29, Position 57: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  38 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
None
== [models/prep/prep_top_players_present.sql] FAIL
L:  49 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
== [models/ml/ml_moneyline_bins.sql] FAIL
L:  20 | P:   1 | LT02 | Expected indent of 4 spaces. [layout.indent]
L:  21 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  22 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  23 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  24 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  25 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  26 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  27 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  28 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  29 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  30 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  31 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  32 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  33 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  33 | P:  13 | LT02 | Expected line break and indent of 20 spaces before
                       | 'when'. [layout.indent]
L:  34 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  34 | P:  30 | LT02 | Expected line break and indent of 12 spaces before
                       | 'end'. [layout.indent]
L:  35 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  36 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  37 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  38 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  38 | P:  10 | LT02 | Expected line break and indent of 20 spaces before
                       | 'home_moneyline'. [layout.indent]
L:  39 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  39 | P:  13 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  39 | P:  52 | LT02 | Expected line break and indent of 16 spaces before ')'.
                       | [layout.indent]
L:  40 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  40 | P:   9 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  40 | P:  13 | LT02 | Expected line break and indent of 20 spaces before
                       | 'away_moneyline'. [layout.indent]
L:  41 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  41 | P:  13 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  41 | P:  52 | LT02 | Expected line break and indent of 16 spaces before ')'.
                       | [layout.indent]
L:  42 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  43 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  44 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  45 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  46 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  47 | P:   1 | LT02 | Expected indent of 4 spaces. [layout.indent]
L:  53 | P:  11 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  60 | P:  10 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  69 | P:  11 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  74 | P:  45 |  PRS | Line 1129, Position 2: Found unparsable section: 'as
                       | current_date'
All Finished!

sqlfluff-fix.............................................................Failed
- hook id: sqlfluff-fix
- exit code: 1
- files were modified by this hook

=== [dbt templater] Sorting Nodes...
=== [dbt templater] Compiling dbt project...
=== [dbt templater] Project Compiled.
== [models/ml/ml_past_games_analysis_daily.sql] FAIL
L:  51 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  54 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  60 | P:   5 | LT09 | Select targets should be on a new line unless there is
                       | only one select target. [layout.select_targets]
L:  60 | P:  20 | LT02 | Expected line break and indent of 8 spaces before '*'.
                       | [layout.indent]
L: 101 | P:  25 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 102 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L: 102 | P:  34 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 103 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L: 103 | P:  36 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L: 104 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
L: 110 | P:  63 |  PRS | Line 114, Position 63: Found unparsable section: "ROWS
                       | BETWEEN '6' PRECEDING AND CURRENT R..."
L: 112 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
== [models/prep/prep_bans.sql] FAIL
L:  65 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
== [models/prep/prep_player_stats_rolling_avg.sql] FAIL
L:  26 | P:  50 |  PRS | Line 26, Position 50: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  27 | P:  62 |  PRS | Line 27, Position 62: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  28 | P:  61 |  PRS | Line 28, Position 61: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  29 | P:  57 |  PRS | Line 29, Position 57: Found unparsable section: "ROWS
                       | BETWEEN '9' PRECEDING AND CURRENT R..."
L:  38 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
None
== [models/prep/prep_top_players_present.sql] FAIL
L:  49 | P:   5 | AM06 | Inconsistent column references in 'GROUP BY/ORDER BY'
                       | clauses. [ambiguous.column_references]
== [models/ml/ml_betting_strategy.sql] FAIL
L: 110 | P:   1 | LT02 | Expected indent of 4 spaces. [layout.indent]
L: 130 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
== [models/ml/ml_moneyline_bins.sql] FAIL
L:  20 | P:   1 | LT02 | Expected indent of 4 spaces. [layout.indent]
L:  21 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  22 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  23 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  24 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  25 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  26 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  27 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  28 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  29 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  30 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  31 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  32 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  33 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  33 | P:  13 | LT02 | Expected line break and indent of 20 spaces before
                       | 'when'. [layout.indent]
L:  34 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  34 | P:  30 | LT02 | Expected line break and indent of 12 spaces before
                       | 'end'. [layout.indent]
L:  35 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  36 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  37 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  38 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  38 | P:  10 | LT02 | Expected line break and indent of 20 spaces before
                       | 'home_moneyline'. [layout.indent]
L:  39 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  39 | P:  13 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  39 | P:  52 | LT02 | Expected line break and indent of 16 spaces before ')'.
                       | [layout.indent]
L:  40 | P:   1 | LT02 | Expected indent of 16 spaces. [layout.indent]
L:  40 | P:   9 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  40 | P:  13 | LT02 | Expected line break and indent of 20 spaces before
                       | 'away_moneyline'. [layout.indent]
L:  41 | P:   1 | LT02 | Expected indent of 20 spaces. [layout.indent]
L:  41 | P:  13 | CP01 | Keywords must be lower case. [capitalisation.keywords]
L:  41 | P:  52 | LT02 | Expected line break and indent of 16 spaces before ')'.
                       | [layout.indent]
L:  42 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  43 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  44 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  45 | P:   1 | LT02 | Expected indent of 12 spaces. [layout.indent]
L:  46 | P:   1 | LT02 | Expected indent of 8 spaces. [layout.indent]
L:  47 | P:   1 | LT02 | Expected indent of 4 spaces. [layout.indent]
L:  53 | P:  11 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  60 | P:  10 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  69 | P:  11 | LT01 | Unnecessary trailing whitespace. [layout.spacing]
L:  74 | P:  45 |  PRS | Line 1129, Position 2: Found unparsable section: 'as
                       | current_date'
All Finished!

sqlfluff-fix.............................................................Failed
- hook id: sqlfluff-fix
- exit code: 1
- files were modified by this hook

==== finding fixable violations ====
FORCE MODE: Attempting fixes...
=== [dbt templater] Sorting Nodes...
=== [dbt templater] Compiling dbt project...
=== [dbt templater] Project Compiled.
WARNING    Skipped file /home/jacob/Documents/nba_elt_dbt/models/ml/ml_betting_strategy.sql because it is disabled 
WARNING    Skipped file /home/jacob/Documents/nba_elt_dbt/models/ml/ml_moneyline_bins.sql because it is disabled 
WARNING    Skipped file /home/jacob/Documents/nba_elt_dbt/models/ml/ml_past_games_analysis_daily.sql because it is disabled 
WARNING    Skipped file /home/jacob/Documents/nba_elt_dbt/models/ml/ml_past_games_analysis_total_aggs.sql because it is disabled 
None
  [4 templating/parsing errors found]
==== no fixable linting violations found ====
All Finished!
==== lint for unfixable violations ====
== [models/ad_hoc_analytics/bet_probs.sql] PASS
== [models/ad_hoc_analytics/blown_leads_dynamic.sql] PASS
== [models/ad_hoc_analytics/three_pt_shooters.sql] PASS
== [models/marts/contract_value_analysis.sql] PASS
== [models/marts/game_types.sql] PASS
== [models/marts/injury_tracker.sql] PASS
== [models/marts/mov.sql] PASS
== [models/marts/odds_winners_losers.sql] PASS
== [models/marts/pbp.sql] PASS
== [models/marts/player_stats.sql] PASS
== [models/marts/recent_games_players.sql] PASS
== [models/marts/recent_games_teams.sql] PASS
== [models/marts/rolling_avg_stats.sql] PASS
== [models/marts/schedule.sql] PASS
== [models/marts/shooting_stats.sql] PASS
== [models/marts/team_game_types.sql] PASS
== [models/marts/team_ratings.sql] PASS
== [models/marts/user_past_predictions.sql] PASS
== [models/ml/ml_betting_strategy.sql] PASS
== [models/ml/ml_moneyline_bins.sql] PASS
== [models/ml/ml_past_games.sql] PASS
== [models/ml/ml_past_games_analysis_daily.sql] PASS
== [models/ml/ml_past_games_analysis_total_aggs.sql] PASS
== [models/ml/ml_past_games_odds_analysis.sql] PASS
== [models/ml/ml_tonights_games.sql] PASS
== [models/ml/schedule_tonights_games.sql] PASS
== [models/prep/prep_bans.sql] PASS
== [models/prep/prep_boxscores_mvp_calc.sql] PASS
== [models/prep/prep_contract_value_analysis.sql] PASS
== [models/prep/prep_injury_tracker.sql] PASS
== [models/prep/prep_past_schedule_analysis.sql] PASS
== [models/prep/prep_pbp_table.sql] PASS
== [models/prep/prep_player_most_recent_team.sql] PASS
== [models/prep/prep_player_stats.sql] PASS
== [models/prep/prep_player_stats_rolling_avg.sql] PASS
== [models/prep/prep_recent_games_players.sql] PASS
== [models/prep/prep_recent_games_teams.sql] PASS
== [models/prep/prep_reddit_comments.sql] PASS
== [models/prep/prep_reddit_team_sentiment.sql] PASS
== [models/prep/prep_schedule_analysis.sql] PASS
== [models/prep/prep_schedule_table.sql] PASS
== [models/prep/prep_standings_table.sql] PASS
== [models/prep/prep_team_blown_leads.sql] PASS
== [models/prep/prep_team_contracts_analysis.sql] PASS
== [models/prep/prep_team_games_played.sql] PASS
== [models/prep/prep_top_players_present.sql] PASS
== [models/staging/staging_aws_boxscores_incremental_table.sql] PASS
== [models/staging/staging_aws_injury_data_table.sql] PASS
== [tests/assert_moneyline_odds.sql] PASS



```