{{ config(materialized='view', tags=['GOLD', 'OPS', 'DAILY']) }}

/*
Read-only view over legacy append-only history table gold.ingestion_freshness_history.
Query this for historical ingestion freshness trends from the prior mart.
*/

select *
from {{ source('gold', 'ingestion_freshness_history') }}
