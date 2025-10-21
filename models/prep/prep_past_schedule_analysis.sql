with my_cte as (
    select
        date as game_date,
        away_team_acronym as away_team,
        home_team_acronym as home_team
    from {{ ref('prep_schedule_table') }}
    where season_type = 'Regular Season'
),

away as (
    select
        away_team as team,
        home_team as opp,
        game_date,
        'join' as join_col,
        'road' as location_new
    from my_cte
),

home as (
    select
        home_team as team,
        away_team as opp,
        game_date,
        'join' as join_col,
        'home' as location_new
    from my_cte
),

final1 as (
    select *
    from away
    union
    select *
    from home
),

team_status as (
    select
        team,
        team_status,
        wins,
        games_played
    from {{ ref('prep_standings_table') }}

),

opp_status as (
    select
        team as opp,
        team_status as team_status_opp,
        wins as wins_opp,
        games_played as games_played_opp
    from {{ ref('prep_standings_table') }}
),

final2 as (
    select
        f.team,
        t.wins,
        f.opp,
        o.wins_opp,
        f.game_date::date,
        f.location_new,
        t.team_status,
        t.games_played,
        o.games_played_opp,
        o.team_status_opp
    from final1 as f
        left join team_status as t using (team)
        left join opp_status as o using (opp)
),

win_loss as (
    select distinct
        team,
        game_date as date,
        location,
        outcome
    from {{ ref('fact_boxscores') }}
),

combo as (
    select
        final2.*,
        win_loss.location,
        win_loss.outcome,
        case when game_date >= current_date then 'future' else 'past' end as game_status,
        case
            when outcome = 'W' then 1
            else 0
        end as outcome_int,
        case
            when location = 'H' then 1
            else 0
        end as home_games_played,
        case
            when location = 'A' then 1
            else 0
        end as road_games_played,
        case
            when team_status_opp = 'Above .500' then 1
            else 0
        end as above_games_played,
        case
            when team_status_opp = 'Below .500' then 1
            else 0
        end as below_games_played
    from final2
        left join win_loss
            on
                final2.team = win_loss.team
                and final2.game_date::date = win_loss.date::date

    order by game_date
),

home_games_played as (
    select
        team,
        sum(home_games_played) as home_gp
    from combo
    where game_status = 'past'
    group by team
),

road_games_played as (
    select
        team,
        sum(road_games_played) as road_gp
    from combo
    where game_status = 'past'
    group by team
),

home_wins as (
    select
        team,
        sum(outcome_int) as home_wins
    from combo
    where location = 'H'
    group by team
),

road_wins as (
    select
        team,
        sum(outcome_int) as road_wins
    from combo
    where location = 'A'
    group by team
),

above_games_played as (
    select
        team,
        sum(above_games_played) as above_gp
    from combo
    where game_status = 'past'
    group by team
),

below_games_played as (
    select
        team,
        sum(below_games_played) as below_gp
    from combo
    where game_status = 'past'
    group by team
),

below_wins as (
    select
        team,
        sum(outcome_int) as below_500_wins
    from combo
    where team_status_opp = 'Below .500'
    group by team
),

above_wins as (
    select
        team,
        sum(outcome_int) as above_500_wins
    from combo
    where team_status_opp = 'Above .500'
    group by team
),

prefinal as (
    select
        *,
        round((wins::numeric / nullif(games_played::numeric, 0)), 3)::numeric as win_pct,
        round((wins_opp::numeric / nullif(games_played_opp::numeric, 0)), 3)::numeric as win_pct_opp,
        round((above_gp::numeric / nullif(games_played::numeric, 0)), 3)::numeric as pct_vs_above_500,
        home_gp - coalesce(home_wins, 0) as home_losses,
        road_gp - coalesce(road_wins, 0) as road_losses,
        below_gp - below_500_wins as below_500_losses,
        above_gp - above_500_wins as above_500_losses
    from combo
        left join home_games_played using (team)
        left join home_wins using (team)
        left join road_games_played using (team)
        left join road_wins using (team)
        left join above_games_played using (team)
        left join above_wins using (team)
        left join below_games_played using (team)
        left join below_wins using (team)
),

prefinal2 as (
    select
        team,
        game_date,
        location_new,
        wins,
        opp,
        wins_opp,
        team_status,
        games_played,
        games_played_opp,
        team_status_opp,
        location,
        outcome,
        game_status,
        outcome_int,
        home_games_played,
        road_games_played,
        above_games_played,
        below_games_played,
        home_gp,
        road_gp,
        above_gp,
        above_500_wins,
        below_gp,
        below_500_wins,
        win_pct,
        win_pct_opp,
        home_losses,
        road_losses,
        below_500_losses,
        above_500_losses,
        pct_vs_above_500,
        coalesce(home_wins, 0) as home_wins,
        coalesce(road_wins, 0) as road_wins,
        games_played - wins as losses
    from prefinal
),

opp_avg_win_pct as (
    select
        team,
        round(avg(win_pct_opp::numeric), 3)::numeric as avg_win_pct_opp
    from prefinal2
    where game_status = 'past'
    group by team
),

final as (
    select
        *,
        1 - pct_vs_above_500 as pct_vs_below_500,
        concat(road_wins, ' - ', road_losses) as road_record,
        concat(home_wins, ' - ', home_losses) as home_record,
        concat(above_500_wins, ' - ', above_500_losses) as above_record,
        concat(below_500_wins, ' - ', below_500_losses) as below_record,
        concat(wins, ' - ', losses) as record
    from prefinal2
        left join opp_avg_win_pct using (team)
)

select *
from final
/* do counts for everything and then make prod table */
