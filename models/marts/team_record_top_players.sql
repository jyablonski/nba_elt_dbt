with final as (
    select *
    from {{ ref('prep_team_top_player_stats') }}
)

select *
from final
