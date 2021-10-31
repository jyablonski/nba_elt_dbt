select 
    {{dbt_utils.star(from = ref('staging_aws_reddit_data_table'), except = ["num_comments"])}}
from {{ ref('staging_aws_reddit_data_table')}}