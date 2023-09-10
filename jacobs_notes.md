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

https://www.datafold.com/blog/accelerating-dbt-core-ci-cd-with-github-actions-a-step-by-step-guide
https://stackoverflow.com/questions/75286648/multiple-profiles-in-one-profiles-yml-is-possible
https://discourse.getdbt.com/t/how-we-sped-up-our-ci-runs-by-10x-using-slim-ci/2603
https://github.com/RealSelf/dbt-source/blob/development/sample.profiles.yml

`dbt build --select config.materialized:view --target prod`