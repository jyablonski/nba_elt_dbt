with team_blown_leads as (
    select 
        team,
        season_type,
        count(*) as blown_leads_10pt,
        row_number() over (partition by season_type order by season_type, count(*) desc) as blown_lead_rank
    from {{ ref('prep_team_blown_leads') }}
    where max_team_lead >= 10 and outcome = 'L' and season_type in ('Regular Season', 'Playoffs')
    group by team, season_type
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
        distinct 
        p.team,
        p.season_type,
        coalesce(team_blown_leads.blown_leads_10pt, 0) as blown_leads_10pt,
        {{ generate_ord_numbers('coalesce(team_blown_leads.blown_lead_rank, 30)') }} as blown_lead_rank,
        coalesce(team_comebacks.team_comebacks_10pt, 0) as team_comebacks_10pt,
        {{ generate_ord_numbers('coalesce(team_comebacks.comeback_rank, 30)') }} as comeback_rank
    from {{ ref('prep_team_blown_leads') }} as p
    left join team_comebacks using (team, season_type)
    left join team_blown_leads using (team, season_type)
    where p.season_type in ('Regular Season', 'Playoffs')
    order by team_comebacks_10pt desc

),

final2 as (
    select 
        *,
        team_comebacks_10pt - blown_leads_10pt as net_comebacks,
        {{ generate_ord_numbers('row_number() over (partition by season_type order by team_comebacks_10pt - blown_leads_10pt desc)') }} as net_rank
    from final
    order by season_type, net_comebacks desc
)

select 
    *
from final2