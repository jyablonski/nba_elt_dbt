with final as (
    select *
    from {{ ref('int_team_top_player_stats') }}
)

select *
from final
