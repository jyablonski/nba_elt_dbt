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
        active_protocols,
        wins_last_10,
        losses_last_10,
        row_number() over (partition by conference order by round((wins::numeric / games_played::numeric), 3)::numeric desc) as rank
    from {{ ref('prep_standings_table') }}

),

standings2 as (
    select
        case
            when (rank = 1) and conference = 'Western' then 2  -- this is changing around the seeds for the play-in teams
            when (rank = 2) and conference = 'Western' then 1
            when (rank = 6) and conference = 'Western' then 8
            when (rank = 7) and conference = 'Western' then 6
            when (rank = 8) and conference = 'Western' then 7
            when (rank = 5) and conference = 'Eastern' then 6
            when (rank = 6) and conference = 'Eastern' then 5
            -- when (rank = 9) and (conference = 'Western') then 8
            else rank
        end as rank,
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
    order by conference, rank
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
