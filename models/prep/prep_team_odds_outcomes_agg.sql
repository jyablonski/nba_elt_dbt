with spread_category_counts as (
    select
        team,
        season_type,
        count(*) as games_played,
        sum(case when covered_spread = 1 then 1 else 0 end) as games_covered_spread,  -- Games where team covered the spread
        sum(case when spread < 0 then 1 else 0 end) as games_favorite,  -- Games where team was favored
        sum(case when spread > 0 then 1 else 0 end) as games_underdog,  -- Games where team was an underdog
        sum(case when spread < 0 and covered_spread = 1 then 1 else 0 end) as games_favorite_covered,  -- Favorite & covered
        sum(case when spread > 0 and covered_spread = 1 then 1 else 0 end) as games_underdog_covered  -- Underdog & covered
    from {{ ref('prep_team_odds_outcomes') }}
    group by
        team,
        season_type
),

spread_pcts as (
    select
        team,
        season_type,
        games_played,
        games_covered_spread,
        games_favorite,
        games_underdog,
        games_favorite_covered,
        games_underdog_covered,
        -- Calculate the percentages
        round(games_covered_spread::decimal / nullif(games_played, 0), 3) as pct_covered_spread,  -- Percentage of games covered the spread
        round(games_favorite_covered::decimal / nullif(games_favorite, 0), 3) as pct_favorite_covered,  -- Percentage of favorite games covered
        round(games_underdog_covered::decimal / nullif(games_underdog, 0), 3) as pct_underdog_covered  -- Percentage of underdog games covered
    from spread_category_counts
)

select *
from spread_pcts
