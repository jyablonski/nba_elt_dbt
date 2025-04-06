FROM python:3.12-slim AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/.venv/bin:/root/.cargo/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=3.12

# install OS-level deps once in base (so we don't repeat in each stage)
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install --no-install-recommends \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------
# Stage: Install Python deps
# -------------------------------
FROM base AS python-deps

COPY pyproject.toml uv.lock ./

RUN pip install uv==0.6.12 && \
    uv sync --frozen --no-dev

# -------------------------------
# Stage: Runtime
# -------------------------------
FROM base AS runtime

COPY --from=python-deps /.venv ./.venv

# reminder: can't do any `RUN useradd --create-home appuser` in dbt
# because you'll hit a permissions error when running dbt deps
WORKDIR /app

COPY . .

RUN dbt deps --profiles-dir profiles/

CMD ["dbt", "build", "--profiles-dir", "profiles/", "--target", "prod"]
