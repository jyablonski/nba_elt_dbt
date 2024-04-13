with most_recent_date as (
    select max(scrape_date) as scrape_date
    from {{ source('nba_source', 'aws_adv_stats_source') }}
),

adv_stats as (
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
        aws_adv_stats_source.scrape_date::date as scrape_date,
        row_number() over (order by nrtg::numeric desc) as nrtg_order,
        row_number() over (order by ortg::numeric desc) as ortg_order,
        row_number() over (order by drtg::numeric) as drtg_order,
        row_number() over (order by srs::numeric desc) as srs_order,
        row_number() over (order by pace::numeric desc) as pace_order,
        row_number() over (order by "ts%"::numeric desc) as ts_percent_order,
        row_number() over (order by "tov%_opp"::numeric desc) as tov_percent_opp_order,
        row_number() over (order by "efg%_opp"::numeric) as efg_percent_opp_order,
        row_number() over (order by "ft/fga_opp" ::numeric) as ft_fga_opp_order,
        created_at,
        modified_at
    from {{ source('nba_source', 'aws_adv_stats_source') }}
        inner join most_recent_date on aws_adv_stats_source.scrape_date = most_recent_date.scrape_date
)


select
    team,
    scrape_date,
    age,
    w,
    l,
    pw,
    pl,
    mov,
    sos,
    srs,
    ortg,
    drtg,
    nrtg,
    pace,
    ftr,
    three_p_rate,
    ts_percent,
    efg_percent,
    tov_percent,
    orb_percent,
    ft_fga,
    efg_percent_opp,
    tov_percent_opp,
    drb_percent_opp,
    ft_fga_opp,
    {{ generate_ord_numbers('nrtg_order') }} as nrtg_rank,
    {{ generate_ord_numbers('ortg_order') }} as ortg_rank,
    {{ generate_ord_numbers('drtg_order') }} as drtg_rank,
    {{ generate_ord_numbers('srs_order') }} as srs_rank,
    {{ generate_ord_numbers('pace_order') }} as pace_rank,
    {{ generate_ord_numbers('ts_percent_order') }} as ts_percent_rank,
    {{ generate_ord_numbers('tov_percent_opp_order') }} as tov_percent_opp_rank,
    {{ generate_ord_numbers('efg_percent_opp_order') }} as efg_percent_opp_rank,
    {{ generate_ord_numbers('ft_fga_opp_order') }} as ft_fga_opp_rank
from adv_stats
