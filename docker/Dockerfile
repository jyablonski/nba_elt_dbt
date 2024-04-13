FROM python:3.11-slim as base

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1

FROM base AS python-deps

COPY docker/install_packages.sh .
COPY pyproject.toml .
COPY poetry.lock .

RUN ./install_packages.sh
RUN pip install poetry
RUN poetry install --without dev

FROM base AS runtime

COPY --from=python-deps ./.venv ./.venv
ENV PATH="/.venv/bin:$PATH"

RUN useradd --create-home appuser
WORKDIR /home/appuser

# USER appuser
# ^ cant use this bc some folder permission issue with dbt
# 0 stackoverflows or slack posts about it, so fuck it

COPY . .

RUN dbt deps --profiles-dir profiles/

CMD ["dbt", "build", "--profiles-dir", "profiles/", "--target", "prod"]