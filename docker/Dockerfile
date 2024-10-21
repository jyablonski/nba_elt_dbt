FROM python:3.11-slim AS base

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1

FROM base AS python-deps

WORKDIR /app
COPY docker/install_packages.sh pyproject.toml poetry.lock ./

RUN ./install_packages.sh && \
    pip install poetry && \
    poetry install --without dev

FROM base AS runtime

WORKDIR /app
COPY --from=python-deps /app/.venv ./.venv
ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home appuser

WORKDIR /home/appuser
COPY . .

RUN dbt deps --profiles-dir profiles/

# Default command to run
CMD ["dbt", "build", "--profiles-dir", "profiles/", "--target", "prod"]