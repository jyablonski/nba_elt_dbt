{# {% snapshot snapshot_injuries %}

-- use snapshots when the underlying data source doesn't enable you to implement SCD2 dimensions (tracking changes over time).
-- im using upserts on injuries so i'll avoid uploading duplicates but i'll still have to track changes over time
--  i think depending on how i filter the data in staging
{{
    config(
      target_database='jacob_db',
      target_schema='snapshots',
      unique_key='injury_pk',

      strategy='check',
      check_cols=['injury', 'description']
    )
}}

select * from {{ ref('staging_aws_injury_data_table')}}

{% endsnapshot %} #}
