# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm

# ── System dependencies ──────────────────────────────────────────────────────
# unixodbc + msodbcsql18: required for pyodbc / SQL Server connectivity.
# libpq-dev / default-libmysqlclient-dev: not strictly required since we use
# psycopg2-binary and PyMySQL (pure-Python-friendly wheels), but curl/gnupg
# are needed to add Microsoft's ODBC driver repo.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unixodbc \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_etl/ ./sync_etl/

# Runs as a non-root user — this container only ever needs outbound
# network access (to the client's DB and to our API), never elevated
# privileges.
RUN useradd --create-home --shell /bin/bash etluser
USER etluser

ENTRYPOINT ["python", "-m", "sync_etl.main"]
