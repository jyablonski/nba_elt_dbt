with team_attributes as (
    select *
    from {{ ref('dim_teams') }}
),

season_type_scaffold as (
    select 'Regular Season' as season_type
),

all_team_season_combinations as (
    select
        team_attributes.team_acronym as team,
        season_type_scaffold.season_type
    from team_attributes
        cross join season_type_scaffold
),

team_blown_leads as (
    select
        team,
        season_type,
        count(*) as blown_leads_10pt,
        row_number() over (partition by season_type order by season_type, count(*) desc) as blown_lead_rank
    from {{ ref('prep_team_blown_leads') }}
    where max_team_lead >= 10 and outcome = 'L' and season_type in ('Regular Season', 'Playoffs')
    group by
        team,
        season_type
),

team_comebacks as (
    select
        team,
        season_type,
        count(*) as team_comebacks_10pt,
        row_number() over (partition by season_type order by season_type, count(*) desc) as comeback_rank
    from {{ ref('prep_team_blown_leads') }}
    where max_opp_lead >= 10 and outcome = 'W' and season_type in ('Regular Season', 'Playoffs')
    group by team, season_type
),

final as (
    select
        all_team_season_combinations.team,
        all_team_season_combinations.season_type,
        coalesce(team_blown_leads.blown_leads_10pt, 0) as blown_leads_10pt,
        {{ generate_ord_numbers('coalesce(team_blown_leads.blown_lead_rank, 30)') }} as blown_lead_rank,
        coalesce(team_comebacks.team_comebacks_10pt, 0) as team_comebacks_10pt,
        {{ generate_ord_numbers('coalesce(team_comebacks.comeback_rank, 30)') }} as comeback_rank
    from all_team_season_combinations
        left join team_comebacks
            on all_team_season_combinations.team = team_comebacks.team
                and all_team_season_combinations.season_type = team_comebacks.season_type
        left join team_blown_leads
            on all_team_season_combinations.team = team_blown_leads.team
                and all_team_season_combinations.season_type = team_blown_leads.season_type
    order by team_comebacks_10pt desc
),

final2 as (
    select
        *,
        team_comebacks_10pt - blown_leads_10pt as net_comebacks,
        {{ generate_ord_numbers('row_number() over (partition by season_type order by team_comebacks_10pt - blown_leads_10pt desc)') }} as net_rank
    from final
    order by season_type asc, net_comebacks desc
)

select *
from final2
