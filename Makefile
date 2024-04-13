.PHONY: bump-patch
bump-patch:
	@bump2version patch
	@git push --tags
	@git push

.PHONY: bump-minor
bump-minor:
	@bump2version minor
	@git push --tags
	@git push

.PHONY: bump-major
bump-major:
	@bump2version major
	@git push --tags
	@git push

.PHONY: docs-generate
docs-generage:
	@dbt docs generate

.PHONY: docs-serve
docs-serve:
	@dbt docs serve

.PHONY: docker-build
docker-build:
	@docker-compose -f docker/docker-compose.yml build

.PHONY: docker-build-local
docker-build-local:
	docker build -f docker/Dockerfile -t nba_elt_dbt_local .

.PHONY: up
up:
	@docker compose -f docker/docker-compose-postgres.yml up -d postgres

.PHONY: down
down:
	@docker compose -f docker/docker-compose-postgres.yml down

.PHONY: test
test:
	@docker compose -f docker/docker-compose-test.yml down
	@docker compose -f docker/docker-compose-test.yml up --exit-code-from dbt_runner

run_dbt:
	@docker compose -f docker/docker-compose-test.yml run dbt_runner dbt build --profiles-dir profiles/ --profile dbt_ci
