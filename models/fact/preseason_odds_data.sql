{{ config(materialized='incremental') }}

with preseason_odds as (
    select
        aws_preseason_odds_source.team::text,
        aws_team_attributes_source.team_acronym,
        aws_preseason_odds_source.odds::numeric as championship_odds,
        aws_preseason_odds_source.predicted::numeric as predicted_wins,
        82 - aws_preseason_odds_source.predicted as predicted_losses,
        aws_preseason_odds_source.created_at,
        aws_preseason_odds_source.modified_at
    from {{ source('nba_source', 'aws_preseason_odds_source') }}
        left join {{ source('nba_source', 'aws_team_attributes_source') }}
            on
                aws_preseason_odds_source.team = aws_team_attributes_source.team
                {% if is_incremental() %}

                    -- this filter will only be applied on an incremental run
                    -- only grab records where date is greater than the max date of the existing records in the tablegm
                    and aws_preseason_odds_source.modified_at > (select max(modified_at) from {{ this }})

                {% endif %}
)

select *
from preseason_odds
