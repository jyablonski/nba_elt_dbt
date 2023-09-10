{{ config(materialized='view') }}

select *
from {{ ref('transactions') }}
