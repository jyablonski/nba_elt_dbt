with team_blown_leads as (
    select 
        team,
        count(*) as blown_leads_10pt,
        row_number() over (order by count(*) desc) as blown_lead_rank
    from {{ ref('prep_team_blown_leads') }}
    where max_team_lead >= 10 and outcome = 'L'
    group by 1
),

team_comebacks as (
    select 
        team,
        count(*) as team_comebacks_10pt,
        row_number() over (order by count(*) desc) as comeback_rank
    from {{ ref('prep_team_blown_leads') }}
    where max_opp_lead >= 10 and outcome = 'W'
    group by 1
)

select 
    distinct 
    p.team,
    coalesce(team_blown_leads.blown_leads_10pt, 0) as blown_leads_10pt,
    {{ generate_ord_numbers('coalesce(team_blown_leads.blown_lead_rank, 30)') }} as blown_lead_rank,
    coalesce(team_comebacks.team_comebacks_10pt, 0) as team_comebacks_10pt,
    {{ generate_ord_numbers('coalesce(team_comebacks.comeback_rank, 30)') }} as comeback_rank
from {{ ref('prep_team_blown_leads') }} p
left join team_comebacks using (team)
left join team_blown_leads using (team)
order by team_comebacks_10pt desc
