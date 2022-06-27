{% set mov = 10 %}

-- same as the prod blown leads table, just that you can swap margin of victory around interactively.  might be interesting

with home_teams as (
    select distinct
        date,
        season_type,
        game_id,
        home_team as opp,
        home_team_full as opp_full,
        away_team as team,
        away_team_full as team_full,
        'Road' as location,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead,
        case when winning_team = home_team then 'Home'
        else 'Road' end as winning_team_loc
    from {{ ref('prep_pbp_table') }}
    order by date
),

road_teams as (
        select distinct
        date,
        season_type,
        game_id,
        away_team as opp,
        away_team_full as opp_full,
        home_team as team,
        home_team_full as team_full,
        'Home' as location,
        winning_team,
        losing_team,
        max_home_lead,
        max_away_lead,
        case when winning_team = home_team then 'Home'
        else 'Road' end as winning_team_loc
    from {{ ref('prep_pbp_table') }}
    order by date
),

combo as (
    select *
    from road_teams
    union 
    select *
    from home_teams
    order by date, game_id
),

full_table as (
    select 
        *,
        case when location = 'Home' then abs(max_home_lead)
        else abs(max_away_lead) end as max_team_lead,
        case when location = 'Home' then abs(max_away_lead)
        else abs(max_home_lead) end as max_opp_lead,
        case when team = winning_team then 'W'
        else 'L' end as outcome
    from combo
),

team_blown_leads as (
    select 
        team,
        season_type,
        count(*) as blown_leads_{{ mov }}pt,
        row_number() over (partition by season_type order by season_type, count(*) desc) as blown_lead_rank
    from full_table
    where max_team_lead >= {{ mov }} and outcome = 'L' and season_type in ('Regular Season', 'Playoffs')
    group by team, season_type
),

team_comebacks as (
    select 
        team,
        season_type,
        count(*) as team_comebacks_{{ mov }}pt,
        row_number() over (partition by season_type order by season_type, count(*) desc) as comeback_rank
    from full_table
    where max_opp_lead >= {{ mov }} and outcome = 'W' and season_type in ('Regular Season', 'Playoffs')
    group by team, season_type
),

final as (
    select 
        distinct 
        p.team,
        p.season_type,
        coalesce(team_blown_leads.blown_leads_{{ mov }}pt, 0) as blown_leads_{{ mov }}pt,
        {{ generate_ord_numbers('coalesce(team_blown_leads.blown_lead_rank, 30)') }} as blown_lead_rank,
        coalesce(team_comebacks.team_comebacks_{{ mov }}pt, 0) as team_comebacks_{{ mov }}pt,
        {{ generate_ord_numbers('coalesce(team_comebacks.comeback_rank, 30)') }} as comeback_rank
    from full_table as p
    left join team_comebacks using (team, season_type)
    left join team_blown_leads using (team, season_type)
    where p.season_type in ('Regular Season', 'Playoffs')
    order by team_comebacks_{{ mov }}pt desc

),

final2 as (
    select 
        *,
        team_comebacks_{{ mov }}pt - blown_leads_{{ mov }}pt as net_comebacks,
        row_number() over (partition by season_type order by team_comebacks_{{ mov }}pt - blown_leads_{{ mov }}pt desc) as net_rank_numeric
    from final
    order by season_type, net_comebacks desc
)

select 
    *,
    {{ generate_ord_numbers('net_rank_numeric') }} as net_rank
from final2