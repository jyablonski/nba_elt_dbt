.PHONY: docs
docs: up 
	@dbt docs generate
	@dbt docs serve --host 0.0.0.0

.PHONY: docker-build
docker-build:
	@docker-compose -f docker/docker-compose.yml build

.PHONY: docker-build-local
docker-build-local:
	docker build -f docker/Dockerfile -t nba_elt_dbt_local .

.PHONY: up
up:
	@docker compose -f docker/docker-compose-postgres.yml up -d

.PHONY: down
down:
	@docker compose -f docker/docker-compose-postgres.yml down

# exit immediately after dbt runner finishes, and only show dbt runner logs
.PHONY: test
test:
	@docker compose -f docker/docker-compose-test.yml down
	@docker compose -f docker/docker-compose-test.yml up --exit-code-from dbt_runner --attach dbt_runner

run_dbt:
	@docker compose -f docker/docker-compose-test.yml run dbt_runner dbt build --profiles-dir profiles/ --profile dbt_ci

.PHONY: run-unit-tests
run-unit-tests:
	@docker compose -f docker/docker-compose-test.yml run dbt_runner dbt test --select test_type:unit --profiles-dir profiles/ --profile dbt_ci
	@make down


.PHONY: cd-docs-generate
cd-docs-generate:
	@docker compose -f docker/docker-compose-test.yml run dbt_runner dbt docs generate --profiles-dir profiles/ --profile dbt_ci
	@make down

.PHONY: lint
lint:
	@docker compose -f docker/docker-compose-test.yml run dbt_runner dbt run-operation lint --profiles-dir profiles/ --profile dbt_ci
	@make down