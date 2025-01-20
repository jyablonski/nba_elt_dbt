-- edge case where a player shot 2 tech free throws in a row, time quarter play and game description were all the same so test failed.

with recent_date as (
    select max(game_date) as game_date
    from {{ ref('fact_pbp_data') }}
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
        game_date,
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
        losing_team,
        case
            when (scoring_team = leading_team) and (leading_team != 'TIE') then 'Leading'
            when (scoring_team != leading_team) and (leading_team != 'TIE') then 'Trailing'
            else 'TIE'
        end as leading_team_text
    from {{ ref('fact_pbp_data') }}
        inner join recent_date using (game_date)
    order by game_description asc, time_remaining_final desc
)

select *
from final
