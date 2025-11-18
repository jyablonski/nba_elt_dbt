with team_game_stats as (
    select
        team,
        team_full,
        season_type,
        result,
        minutes_leading,
        minutes_trailing,
        minutes_tied,
        total_minutes,
        pct_time_leading,
        pct_time_trailing,
        pct_time_tied,
        biggest_lead,
        biggest_deficit,
        led_most_but_lost,
        trailed_most_but_won,
        home_away,
        game_date,
        lead_changes
    from {{ ref('prep_team_game_lead_times') }}
),

team_aggregations as (
    select
        team,
        team_full,
        season_type,

        -- Game counts
        count(*) as games_played,
        sum(case when result = 'W' then 1 else 0 end) as wins,
        sum(case when result = 'L' then 1 else 0 end) as losses,
        round(sum(case when result = 'W' then 1 else 0 end)::numeric / count(*)::numeric, 3) as win_pct,

        -- Home/Away splits
        sum(case when home_away = 'HOME' then 1 else 0 end) as home_games,
        sum(case when home_away = 'AWAY' then 1 else 0 end) as away_games,
        sum(case when home_away = 'HOME' and result = 'W' then 1 else 0 end) as home_wins,
        sum(case when home_away = 'AWAY' and result = 'W' then 1 else 0 end) as away_wins,

        -- Average time distribution
        round(avg(pct_time_leading), 2) as avg_pct_time_leading,
        round(avg(pct_time_trailing), 2) as avg_pct_time_trailing,
        round(avg(pct_time_tied), 2) as avg_pct_time_tied,

        round(avg(minutes_leading), 2) as avg_minutes_leading,
        round(avg(minutes_trailing), 2) as avg_minutes_trailing,
        round(avg(minutes_tied), 2) as avg_minutes_tied,

        -- Lead statistics
        round(avg(biggest_lead), 2) as avg_biggest_lead,
        max(biggest_lead) as max_biggest_lead,
        round(avg(biggest_deficit), 2) as avg_biggest_deficit,
        max(biggest_deficit) as max_biggest_deficit,

        -- Lead changes metrics
        round(avg(lead_changes), 2) as avg_lead_changes,
        max(lead_changes) as max_lead_changes,

        -- Comeback/clutch stats
        sum(case when led_most_but_lost then 1 else 0 end) as games_led_most_but_lost,
        sum(case when trailed_most_but_won then 1 else 0 end) as games_trailed_most_but_won,
        round(sum(case when trailed_most_but_won then 1 else 0 end)::numeric / nullif(sum(case when result = 'W' then 1 else 0 end)::numeric, 0), 3) as comeback_win_rate,

        -- Close game performance (led <50% of time but won, or led >50% but lost)
        sum(case when pct_time_leading < 50 and result = 'W' then 1 else 0 end) as wins_when_trailing_most,
        sum(case when pct_time_leading > 50 and result = 'L' then 1 else 0 end) as losses_when_leading_most,

        -- Dominance metrics (led >75% of game time)
        sum(case when pct_time_leading > 75 then 1 else 0 end) as games_dominated,
        sum(case when pct_time_leading > 75 and result = 'W' then 1 else 0 end) as dominant_wins,
        round(sum(case when pct_time_leading > 75 and result = 'W' then 1 else 0 end)::numeric / nullif(sum(case when pct_time_leading > 75 then 1 else 0 end)::numeric, 0), 3) as dominant_game_win_rate,

        -- Wire-to-wire performance (led entire game)
        sum(case when pct_time_leading > 95 then 1 else 0 end) as wire_to_wire_games,
        sum(case when pct_time_leading > 95 and result = 'W' then 1 else 0 end) as wire_to_wire_wins,

        -- Trailing performance
        sum(case when pct_time_trailing > 75 then 1 else 0 end) as games_trailed_most,
        sum(case when pct_time_trailing > 75 and result = 'L' then 1 else 0 end) as blowout_losses,

        -- Competitive game metrics (using lead changes instead of tie time)
        -- Competitive = 8+ lead changes OR biggest lead/deficit both < 10 points
        sum(
            case
                when lead_changes >= 8 or (biggest_lead <= 10 and biggest_deficit <= 10)
                    then 1
                else 0
            end
        ) as competitive_games,
        sum(
            case
                when (lead_changes >= 8 or (biggest_lead <= 10 and biggest_deficit <= 10)) and result = 'W'
                    then 1
                else 0
            end
        ) as competitive_wins,
        round(
            sum(case when (lead_changes >= 8 or (biggest_lead <= 10 and biggest_deficit <= 10)) and result = 'W' then 1 else 0 end)::numeric
            / nullif(sum(case when lead_changes >= 8 or (biggest_lead <= 10 and biggest_deficit <= 10) then 1 else 0 end)::numeric, 0),
            3
        ) as competitive_game_win_rate,

        -- High lead change games (back-and-forth)
        sum(case when lead_changes >= 10 then 1 else 0 end) as high_lead_change_games,
        sum(case when lead_changes >= 10 and result = 'W' then 1 else 0 end) as high_lead_change_wins,
        round(
            sum(case when lead_changes >= 10 and result = 'W' then 1 else 0 end)::numeric
            / nullif(sum(case when lead_changes >= 10 then 1 else 0 end)::numeric, 0),
            3
        ) as high_lead_change_win_rate,

        -- Lead efficiency (do they hold leads?)
        round(sum(case when pct_time_leading > 50 and result = 'W' then 1 else 0 end)::numeric / nullif(sum(case when pct_time_leading > 50 then 1 else 0 end)::numeric, 0), 3) as lead_protection_rate,

        -- Comeback ability (do they overcome deficits?)
        round(sum(case when pct_time_trailing > 50 and result = 'W' then 1 else 0 end)::numeric / nullif(sum(case when pct_time_trailing > 50 then 1 else 0 end)::numeric, 0), 3) as comeback_success_rate,

        -- Recent form (last 30 games)
        sum(case when game_date >= current_date - interval '30 days' and result = 'W' then 1 else 0 end) as wins_last_30_days,
        sum(case when game_date >= current_date - interval '30 days' then 1 else 0 end) as games_last_30_days,

        current_timestamp as created_at

    from team_game_stats
    group by
        team,
        team_full,
        season_type
)

select
    team,
    team_full,
    season_type,

    -- Record
    games_played,
    wins,
    losses,
    win_pct,

    -- Home/Away
    home_games,
    away_games,
    home_wins,
    away_wins,
    round(home_wins::numeric / nullif(home_games::numeric, 0), 3) as home_win_pct,
    round(away_wins::numeric / nullif(away_games::numeric, 0), 3) as away_win_pct,

    -- Time distribution averages
    avg_pct_time_leading,
    avg_pct_time_trailing,
    avg_pct_time_tied,
    avg_minutes_leading,
    avg_minutes_trailing,
    avg_minutes_tied,

    -- Lead metrics
    avg_biggest_lead,
    max_biggest_lead,
    avg_biggest_deficit,
    max_biggest_deficit,

    -- Lead change metrics
    avg_lead_changes,
    max_lead_changes,

    -- Clutch/comeback performance
    games_led_most_but_lost,
    games_trailed_most_but_won,
    comeback_win_rate,
    wins_when_trailing_most,
    losses_when_leading_most,

    -- Game style metrics
    games_dominated,
    dominant_wins,
    dominant_game_win_rate,
    wire_to_wire_games,
    wire_to_wire_wins,
    games_trailed_most,
    blowout_losses,
    competitive_games,
    competitive_wins,
    competitive_game_win_rate,
    high_lead_change_games,
    high_lead_change_wins,
    high_lead_change_win_rate,

    -- Efficiency metrics
    lead_protection_rate,
    comeback_success_rate,

    -- Recent form
    wins_last_30_days,
    games_last_30_days,
    round(wins_last_30_days::numeric / nullif(games_last_30_days::numeric, 0), 3) as win_pct_last_30_days,

    created_at

from team_aggregations
order by win_pct desc, team asc
