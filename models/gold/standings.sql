with standings as (
    select
        team,
        team_full,
        conference,
        wins,
        losses,
        games_played,
        round(wins::numeric / nullif(games_played::numeric, 0), 3)::numeric as win_pct,
        active_injuries,
        active_protocols,
        wins_last_10,
        losses_last_10,
        season_rank as rank

    from {{ ref('int_standings_table') }}

),

standings2 as (
    select
        rank,
        team,
        team_full,
        conference,
        wins,
        losses,
        games_played,
        win_pct,
        active_injuries,
        active_protocols,
        concat(wins_last_10, '-', losses_last_10) as last_10
    from standings
    order by
        conference,
        rank
),

final as (
    select
        {{ generate_ord_numbers('rank') }} as rank,
        team,
        team_full,
        conference,
        wins,
        losses,
        games_played,
        win_pct,
        active_injuries,
        active_protocols,
        last_10
    from standings2
)


select *
from final

/* top 20 pt scorers contract avlue analysis team contract value analysis .  add standings (5-11) to mov
  # wholeeee schedle analysis plots.
  # team bans stuff (wins, losses, seed in east/west, last season wins
  # game types data base plot from mov)
  # opponent shooting bans
  # home road team stuff + team win %s vs above below .500
*/
