with most_recent_date as (
    select max(scrape_date) as max_scrape_date
    from {{ source('nba_source', 'aws_stats_source') }}
)

select
    coalesce(player, '')::text as player,
    coalesce(pos, '')::text as pos,
    coalesce(nullif(age, '')::integer, 0) as age,
    coalesce(team, '')::text as team,
    coalesce(nullif(g, '')::integer, 0) as g,
    coalesce(nullif(gs, '')::integer, 0) as gs,
    coalesce(mp, '')::text as mp,
    coalesce(nullif(fg, '')::numeric, 0) as fg,
    coalesce(nullif(fga, '')::numeric, 0) as fga,
    coalesce(nullif("fg%", '')::numeric, 0) as fg_percent,     -- Handle fg% by using NULLIF first
    coalesce(nullif("3p", '')::numeric, 0) as three_p,
    coalesce(nullif("3pa", '')::numeric, 0) as three_p_attempted,
    coalesce(nullif("3p%", '')::numeric, 0) as three_p_percent, -- Apply the same for all percentage columns
    coalesce(nullif("2p", '')::numeric, 0) as two_p,
    coalesce(nullif("2pa", '')::numeric, 0) as two_p_attempted,
    coalesce(nullif("2p%", '')::numeric, 0) as two_p_percent,
    coalesce(nullif("efg%", '')::numeric, 0) as efg_percent,
    coalesce(nullif("ft%", '')::numeric, 0) as ft_percent,
    coalesce(nullif(orb, '')::numeric, 0) as orb,
    coalesce(nullif(drb, '')::numeric, 0) as drb,
    coalesce(nullif(trb, '')::numeric, 0) as trb,
    coalesce(nullif(ast, '')::numeric, 0) as ast,
    coalesce(nullif(stl, '')::numeric, 0) as stl,
    coalesce(nullif(blk, '')::numeric, 0) as blk,
    coalesce(nullif(tov, '')::numeric, 0) as tov,
    coalesce(nullif(pf, '')::numeric, 0) as pf,
    coalesce(pts, 0) as pts,
    scrape_date::date

from {{ source('nba_source', 'aws_stats_source') }}
    inner join most_recent_date on aws_stats_source.scrape_date = most_recent_date.max_scrape_date
where player is not null
