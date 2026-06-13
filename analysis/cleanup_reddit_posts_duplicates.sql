/*
One-time cleanup for duplicate reddit posts.

Keeps the latest row per (reddit_url, scrape_date) using created_at, then
modified_at as tiebreakers. Run against prod, then:

  dbt run --full-refresh --select fact_reddit_posts
*/

-- Bronze source
delete from bronze.reddit_posts as stale
using bronze.reddit_posts as keeper
where stale.reddit_url = keeper.reddit_url
    and stale.scrape_date = keeper.scrape_date
    and stale.reddit_url is not null
    and stale.scrape_date is not null
    and (
        keeper.created_at > stale.created_at
        or (
            keeper.created_at = stale.created_at
            and keeper.modified_at > stale.modified_at
        )
        or (
            keeper.created_at is not distinct from stale.created_at
            and keeper.modified_at is not distinct from stale.modified_at
            and keeper.ctid > stale.ctid
        )
    );

-- Silver fact (optional if not doing a full refresh immediately)
delete from silver.fact_reddit_posts as stale
using silver.fact_reddit_posts as keeper
where stale.reddit_url = keeper.reddit_url
    and stale.scrape_date = keeper.scrape_date
    and stale.reddit_url is not null
    and stale.scrape_date is not null
    and (
        keeper.created_at > stale.created_at
        or (
            keeper.created_at = stale.created_at
            and keeper.modified_at > stale.modified_at
        )
        or (
            keeper.created_at is not distinct from stale.created_at
            and keeper.modified_at is not distinct from stale.modified_at
            and keeper.ctid > stale.ctid
        )
    );

-- Verify no duplicates remain
select
    'bronze.reddit_posts' as table_name,
    count(*) as duplicate_groups
from (
    select reddit_url, scrape_date
    from bronze.reddit_posts
    where reddit_url is not null
        and scrape_date is not null
    group by reddit_url, scrape_date
    having count(*) > 1
) as bronze_dupes

union all

select
    'silver.fact_reddit_posts' as table_name,
    count(*) as duplicate_groups
from (
    select reddit_url, scrape_date
    from silver.fact_reddit_posts
    where reddit_url is not null
        and scrape_date is not null
    group by reddit_url, scrape_date
    having count(*) > 1
) as silver_dupes;
