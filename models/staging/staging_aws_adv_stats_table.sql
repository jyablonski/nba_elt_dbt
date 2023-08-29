with my_cte as (
    select
        team::text,
        age::numeric,
        w::integer,
        l::integer,
        pw::integer,
        pl::integer,
        mov::numeric,
        sos::numeric,
        srs::numeric,
        ortg::numeric,
        drtg::numeric,
        nrtg::numeric,
        pace::numeric,
        ftr::numeric,
        "3par"::numeric as three_p_rate,
        "ts%"::numeric as ts_percent,
        "efg%"::numeric as efg_percent,
        "tov%"::numeric as tov_percent,
        "orb%"::numeric as orb_percent,
        "ft/fga"::numeric as ft_fga,
        "efg%_opp"::numeric as efg_percent_opp,
        "tov%_opp"::numeric as tov_percent_opp,
        "drb%_opp"::numeric as drb_percent_opp,
        "ft/fga_opp"::numeric as ft_fga_opp,
        arena::text,
        attendance::numeric,
        "att/game"::numeric as att_game,
        scrape_date::date as scrape_date
    from {{ source('nba_source', 'aws_adv_stats_source') }}
),

most_recent_date as (
    select max(scrape_date) as scrape_date
    from {{ source('nba_source', 'aws_adv_stats_source') }}
),

final as (
    select *
    from my_cte
        inner join most_recent_date using (scrape_date)
)

select *
from final
