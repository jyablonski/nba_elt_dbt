![Deployment](https://github.com/jyablonski/nba_elt_dbt/actions/workflows/deploy.yml/badge.svg) ![Tests](https://github.com/jyablonski/nba_elt_dbt/actions/workflows/test.yml/badge.svg)

Version: 3.0.5

## dbt Resources for [NBA Project](https://github.com/jyablonski/NBA-Dashboard)

## Running The App
Clone the Repo & run `make up` which will spin up a Postgres Container w/ bootstrapped data to test the project with.

From there you can use Poetry to setup a local Environment w/ the appropriate dbt Packages by running `poetry install`.

You must also have a `~/.dbt/profiles.yml` setup with the following Config:
``` yml
default:
  outputs:
    default:
      type: postgres
      host: localhost
      port: 5432
      dbname: jacob_db
      schema: public
      user: postgres
      password: postgres
```

When finished run `make down` to spin the Postgres Container down.

## Tests
To run tests locally, run `make test`

The same test suite runs on every commit to a PR via GitHub Actions

## NBA Project
![nba_pipeline_diagram](https://github.com/jyablonski/nba_elt_dbt/assets/16946556/044dbb79-ce33-4d4b-8262-357c531e7ce7)

1. Links to other Repos providing infrastructure for this Project
    * [Dash Server](https://github.com/jyablonski/nba_elt_dashboard)
    * [Ingestion Script](https://github.com/jyablonski/nba_elt_ingestion)
    * [Terraform](https://github.com/jyablonski/aws_terraform)
    * [ML Pipeline](https://github.com/jyablonski/nba_elt_mlflow)
    * [REST API](https://github.com/jyablonski/nba_elt_rest_api)
