FROM python:3.8-slim as base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS python-deps

COPY docker/install_packages.sh .
RUN ./install_packages.sh

COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
RUN git clone https://github.com/jyablonski/nba_elt_dbt.git

FROM base AS runtime

COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

RUN useradd --create-home appuser
WORKDIR /home/appuser

COPY --from=python-deps . .
WORKDIR /home/appuser/nba_elt_dbt
COPY .ci_cd .

# RUN ls .
# RUN cat profiles.yml
RUN dbt deps --profiles-dir /

CMD ["dbt", "build", "--profiles-dir", "."]