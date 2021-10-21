/*         COALESCE("3p%"::text::numeric) as three_p_percent, */
with my_cte as (
    SELECT
        player::text,
        pos::text,
        age::integer,
        tm::text,
        g::integer,
        gs::integer,
        mp::text,
        fg::numeric,
        fga::numeric,
        "fg%"AS fg_percent,
        "3p" AS three_p,
        "3pa" AS three_p_attempted,
        "3p%" AS three_p_percent,
        "2p" AS two_p,
        "2pa" AS two_p_attempted,
        "2p%" AS two_p_percent,
        "efg%" AS efg_percent,
        "ft%" AS ft_percent,
        orb::numeric,
        drb::numeric,
        trb::numeric,
        ast::numeric,
        stl::numeric,
        blk::numeric,
        tov::numeric,
        pf::numeric,
        pts::numeric,
        scrape_date::date

FROM {{ source('nba_source', 'aws_stats_source')}}
WHERE player IS NOT NULL
),

most_recent_date as (
    SELECT
        max(scrape_date) as scrape_date
    FROM {{ source('nba_source', 'aws_stats_source')}}
),

final as (
    select *
    from my_cte
    inner join most_recent_date using (scrape_date)
)

select *
from final
