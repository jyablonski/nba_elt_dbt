with my_cte as (
    select distinct
        team,
        date
    from {{ ref('staging_aws_boxscores_incremental_table') }}
    order by team, date
),

-- you play on 2022-10-25 and 2022-10-27, you have 1 day of rest, even tho there's 2 days between those dates.  so do -1.
-- coalesce to replace the initial row for each team (1st game of season)
-- the case when it to just limit it to 4 days of rest by default for ML purposes. - beginning of season, all star break will show 4 days rest
final as (
    select
        a.team,
        c.date,
        case
            when coalesce(date - lag(date) over (partition by a.team order by date), 5)::numeric - 1 > 4 then 4
            else coalesce(date - lag(date) over (partition by a.team order by date), 5)::numeric - 1
        end as days_rest,
        rank() over (partition by a.team order by c.date desc) as rank
    from my_cte as c
        left join {{ ref('staging_seed_team_attributes') }} as a on c.team = a.team_acronym
)

select *
from final
