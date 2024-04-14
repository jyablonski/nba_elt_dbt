with my_data as (
    select distinct
        team,
        game_date,
        location,
        outcome,
        opponent,
        case
            when outcome = 'W' then 1
            else 0
        end as outcome_win
    from {{ ref('boxscores') }}
),

team_ranks as (
    select
        team,
        team_status
    from {{ ref('prep_standings_table') }}
),

opp_ranks as (
    select
        team as opponent,
        team_status as team_status_opp
    from {{ ref('prep_standings_table') }}
),

final as (
    select
        *,
        case
            when game_date < current_date then 'past'
            else 'future'
        end as game_status,
        case
            when outcome_win = 1 then 0
            else 1
        end as outcome_loss
    from my_data
        left join team_ranks using (team)
        left join opp_ranks using (opponent)
)

select *
from final
