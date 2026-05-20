{{ config(materialized='view', tags=['GOLD', 'OPS', 'DAILY']) }}

/*
Read-only view over append-only history populated by ingestion_freshness post_hook.
Query this for trends; gold.ingestion_freshness is today's current snapshot only.
*/

select *
from {{ source('gold', 'ingestion_freshness_history') }}
