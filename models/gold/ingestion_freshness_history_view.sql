{{ config(materialized='view', tags=['GOLD', 'OPS', 'DAILY']) }}

/*
Read-only view over append-only history table gold.ingestion_freshness_history
(populated by ingestion_freshness post_hook). Query this for trends.
*/

select *
from {{ source('gold', 'ingestion_freshness_history') }}
