with standings as (
    select
        team,
        team_full,
        conference,
        wins,
        losses,
        games_played,
        round((wins::numeric / games_played::numeric), 3)::numeric as win_pct,
        active_injuries,
        coalesce(wins_last_10, 0) as wins_last_10,
        coalesce(losses_last_10, 0) as losses_last_10
    from {{ ref('prep_standings_table')}}

),
standings2 as (
    select 
      team,
      team_full,
      conference,
      wins,
      losses,
      games_played,
      win_pct,
      active_injuries,
    concat(wins_last_10, '-', losses_last_10) as last_10
    from standings
)

select *
from standings2

/* top 20 pt scorers contract avlue analysis team contract value analysis .  add standings (5-11) to mov 
  # wholeeee schedle analysis plots.
  # team bans stuff (wins, losses, seed in east/west, last season wins
  # game types data base plot from mov)
  # opponent shooting bans
  # home road team stuff + team win %s vs above below .500
*/