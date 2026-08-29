# Retano360 Sync ETL

Automated 24-hour synchronization from your database into your Retano360
account. This container connects to **your** database, reads the tables
and columns you've configured on the "تنظیم API" page, and sends that
data to Retano360 over HTTPS. It runs once every 24 hours automatically.

## What this does NOT do

- It does **not** send your database credentials to Retano360.
- It does **not** allow Retano360 to connect to your database.
- It does **not** write anything back into your database — read-only,
  one direction only: your database → Retano360.
- It does **not** need to run on the same server as your database or
  website — it only needs network access to reach your database over
  the network, and outbound HTTPS access to reach Retano360's API.

## Prerequisites

- Docker and Docker Compose installed on a server (VPS, cloud instance,
  or any always-on machine — **not** a shared hosting plan, which
  cannot run Docker containers).
- Network access from that server to your database (host, port reachable).
- Your Retano360 sync API key, generated from the "تنظیم API" page after
  filling in every table/column mapping and clicking "تولید API".

## Setup

1. Copy `.env.example` to `.env`:
   ```
   cp .env.example .env
   ```

2. Edit `.env` and fill in:
   - Your database connection details (`DB_ENGINE`, `DB_HOST`, `DB_PORT`,
     `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`)
   - Your Retano360 API URL and API key (`API_URL`, `API_KEY`)

3. Start the container:
   ```
   docker compose up -d
   ```

That's it. The container will:
- Immediately validate it can connect to your database and to Retano360
- Run its first sync cycle
- Then run automatically every 24 hours (configurable via
  `SYNC_INTERVAL_HOURS`)

## Checking logs

```
docker compose logs -f sync-etl
```

Every cycle logs: start time, which tables were queried, how many rows
were read/sent/accepted/rejected, and any errors.

## Table/column name mismatches

If a table or column name you configured on the تنظیم API page does not
actually exist in your database, the sync will **fail entirely** for
that cycle and Retano360 will show an error message on the تنظیم API
page. Check your configuration there and correct any typos — no
redeployment of this container is ever required after fixing a mapping;
the next scheduled cycle will automatically pick up the correction.

**Exception:** for the two optional product attribute fields, a missing
column does not fail the sync — that field is simply left empty for
this run.

## One table per entity

All of the "customer" fields (name, phone, order info, etc.) must come
from a **single table** in your database, and all of the "product"
fields must likewise come from a **single table**. Mixing fields from
multiple tables for the same entity is not supported directly.

If your data is genuinely spread across multiple tables (e.g. customer
contact info in one table, order history in another), create a SQL
**VIEW** in your own database that joins them into one flat result, and
point your تنظیم API mapping at that view's name instead of a raw
table — from this system's perspective a view works exactly like a
table. Your database administrator can set this up once; it never
needs to change afterward unless your schema does.

## Updating your mapping

You never need to touch this container or redeploy it when you change
your table/column mapping on the تنظیم API page. Configuration is
fetched fresh from Retano360 at the start of every 24-hour cycle.

## Running a one-off manual sync (for testing)

```
docker compose run --rm -e RUN_ONCE=true sync-etl
```

This runs a single cycle immediately and exits, instead of starting the
long-running scheduler — useful right after initial setup to confirm
everything is configured correctly before waiting for the next
scheduled cycle.

## Supported databases

- PostgreSQL
- MySQL
- MariaDB
- Microsoft SQL Server

## Updating the container

```
docker compose pull
docker compose up -d
```

## Environment variables reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_ENGINE` | Yes | — | `postgresql`, `mysql`, `mariadb`, or `mssql` |
| `DB_HOST` | Yes | — | Database host |
| `DB_PORT` | Yes | — | Database port |
| `DB_NAME` | Yes | — | Database name |
| `DB_USERNAME` | Yes | — | Database username |
| `DB_PASSWORD` | Yes | — | Database password |
| `API_URL` | Yes | — | Retano360 API base URL |
| `API_KEY` | Yes | — | Your tenant sync API key |
| `SYNC_INTERVAL_HOURS` | No | `24` | Hours between sync cycles |
| `REQUEST_TIMEOUT_SECONDS` | No | `30` | HTTP timeout per request |
| `MAX_RETRIES` | No | `5` | Retry attempts for transient failures |
| `RETRY_BACKOFF_SECONDS` | No | `10` | Initial backoff, doubles each retry |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `RUN_ONCE` | No | `false` | Run one cycle and exit instead of scheduling |
