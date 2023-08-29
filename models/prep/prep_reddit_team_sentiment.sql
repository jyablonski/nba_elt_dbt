-- general idea is to convert flairs into 'team' acronyms, and then analyze consumer sentiment following team wins or losses.
-- if i scrape on 2022-04-15 and the team played on 2022-04-14, then reddit comments from that scrape scrape will reflect their
--       sentiment leading up to or following that game played.  reddit threads are most active right after games are played.

with game_dates as (
    select distinct
        team,
        outcome,
        date as potential_game_date,
        1 as game_date
    from {{ ref('prep_boxscores_mvp_calc') }}
),

-- REVIEW THIS MACRO EVERY COUPLE OF MONTHS
new_comments as (
    select
        *,
        {{ convert_team_names_flairs('flair_final') }} as team
    from {{ ref('prep_reddit_comments') }}
),

aggs as (
    select
        scrape_date,
        team,
        scrape_date - 1 as potential_game_date,
        count(*) as num_comments,
        round(avg(score), 3) as avg_score,
        round(avg(neg), 3) as avg_neg,
        round(avg(neu), 3) as avg_neu,
        round(avg(pos), 3) as avg_pos,
        round(avg(compound), 3) as avg_compound
    from new_comments
    group by scrape_date, scrape_date - 1, team
),

final as (
    select
        team,
        scrape_date,
        potential_game_date,
        num_comments,
        avg_score,
        avg_neu,
        avg_neg,
        avg_pos,
        avg_compound,
        coalesce(game_date, 0) as game_date,
        coalesce(outcome, 'NO GAME') as game_outcome
    from aggs
        left join game_dates using (potential_game_date, team)
    order by team, scrape_date
)

select *
from final
