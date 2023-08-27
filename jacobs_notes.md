# Apuntes
`pip install sqlfluff`
`pip install sqlfluff-templater-dbt`


```
# L016 - line too long
# L031 - avoid table aliasessss
# L034 - use wildcards first in select like select *, a * 2, b * 4 etc
```

`sqlfluff lint models/marts/bans.sql`
`sqlfluff fix models/marts/bans.sql`

stackoverflow how to fix 


[sqlfluff default config](https://docs.sqlfluff.com/en/stable/configuration.html#default-configuration)


```
    entry: bash -c 'env DBT_PRAC_KEY=hooks'
    additional_dependencies: [
    'sqlfluff==2.3.0',
    'sqlfluff-templater-dbt==2.3.0',
    'dbt-core==1.5.0',
    'dbt-postgres==1.5.0'
    ]
```
