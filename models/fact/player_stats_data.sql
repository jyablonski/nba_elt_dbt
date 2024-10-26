with most_recent_date as (
    select max(scrape_date) as max_scrape_date
    from {{ source('nba_source', 'aws_stats_source') }}
)

select
    player::text,
    pos::text,
    age::integer,
    team::text,
    g::integer,
    gs::integer,
    mp::text,
    fg::numeric,
    fga::numeric,
    "fg%" as fg_percent,
    "3p" as three_p,
    "3pa" as three_p_attempted,
    "3p%" as three_p_percent,
    "2p" as two_p,
    "2pa" as two_p_attempted,
    "2p%" as two_p_percent,
    "efg%" as efg_percent,
    "ft%" as ft_percent,
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

from {{ source('nba_source', 'aws_stats_source') }}
    inner join most_recent_date on aws_stats_source.scrape_date = most_recent_date.max_scrape_date
where player is not null
