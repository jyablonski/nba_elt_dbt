SELECT *
FROM {{ ref('staging_aws_contracts_table') }}
WHERE team = 'NEED_A_LIST_OF_ACCEPTABLE_TEAM_VALUES'

/* the idea being that we want this to return 0 rows, if something gets returned that means its passing further downstream which is a problem */