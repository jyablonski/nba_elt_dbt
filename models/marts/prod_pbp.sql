with recent_date as (
    select max(date) as date
    from {{ ref('prep_pbp_table') }}
),

final as (
    select
        time_quarter,
        play,
        time_remaining_final,
        quarter,
        away_score,
        score,
        home_score,
        home_team,
        away_team,
        score_away,
        score_home,
        margin_score,
        date,
        leading_team,
        home_team_full,
        home_primary_color,
        away_team_full,
        away_primary_color,
        game_description,
        away_fill,
        home_fill,
        scoring_team_color,
        scoring_team,
        max_home_lead,
        max_away_lead,
        winning_team,
        losing_team

    from {{ ref('prep_pbp_table')}}
    inner join recent_date using (date)
    order by game_description, time_remaining_final desc
)

select 
    *
from final

