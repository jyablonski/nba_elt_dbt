FROM python:3.13-slim AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/.venv/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 \ 
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=3.13 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
# uv installs .pyc files in site-packages, and python doesn't recompile them at runtime

# Install OS-level deps once in base (so we don't repeat in each stage)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------
# Stage: Install Python deps
# -------------------------------
FROM base AS python-deps

COPY pyproject.toml uv.lock ./

# Cache mount: Persist pip's download cache across builds for faster rebuilds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install uv==0.9.3

# Cache mount: Persist uv's download cache across builds
# Bind mounts: Temporarily mount files during RUN without copying them into the layer
# This improves layer caching - if only app code changes, this layer is reused
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-dev

# Copy profiles and dbt_project.yml, then install dbt packages
COPY profiles/ ./profiles/
COPY dbt_project.yml ./dbt_project.yml

RUN dbt deps --profiles-dir profiles/

# -------------------------------
# Stage: Runtime
# -------------------------------
FROM python:3.13-slim AS runtime

ENV PATH="/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY . .

COPY --from=python-deps /.venv /.venv
COPY --from=python-deps /dbt_packages /app/dbt_packages

CMD ["dbt", "build", "--profiles-dir", "profiles/", "--target", "prod"]