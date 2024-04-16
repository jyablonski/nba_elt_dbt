{{ config(materialized='incremental') }}

with shooting_stats as (
    select
        {{ clean_player_names_bbref('player') }}::text as player,
        avg_shot_distance::numeric as avg_shot_distance,
        pct_fga_2p::numeric as pct_fga_2p,
        pct_fga_0_3::numeric as pct_fga_0_3,
        pct_fga_3_10::numeric as pct_fga_3_10,
        pct_fga_10_16::numeric as pct_fga_10_16,
        pct_fga_16_3p::numeric as pct_fga_16_3p,
        pct_fga_3p::numeric as pct_fga_3p,
        fg_pct_0_3::numeric as fg_pct_0_3,
        fg_pct_3_10::numeric as fg_pct_3_10,
        fg_pct_10_16::numeric as fg_pct_10_16,
        fg_pct_16_3p::numeric as fg_pct_16_3p,
        pct_2pfg_ast::numeric as pct_2pfg_ast,
        pct_3pfg_ast::numeric as pct_3pfg_ast,
        dunk_pct_tot_fg::numeric as dunks_pct_tot_fg,
        dunks::integer as dunks,
        corner_3_ast_pct::numeric as corner_3_ast_pct,
        corner_3pm_pct::numeric as corner_3pm_pct,
        heaves_att::numeric as heaves_att,
        heaves_makes::numeric as heaves_makes,
        scrape_date::date as scrape_date,
        scrape_ts::timestamp as scrape_ts,
        created_at,
        modified_at

    from {{ source('nba_source', 'aws_shooting_stats_source') }}
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- only grab records where date is greater than the max date of the existing records in the tablegm
        where created_at > (select max(created_at) from {{ this }})

    {% endif %}
)

select *
from shooting_stats
