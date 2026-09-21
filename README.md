# Web Table to Warehouse: ETL Pipeline

A parameterised ETL pipeline that scrapes a table from a website, converts its values into a chosen
currency, and publishes the result to a lakehouse and a Postgres database. It runs as a three-task
workflow on Azure Databricks.

The source page, target table, currency and destinations are all job parameters, so the same code
runs against a different source without edits. It currently runs against a Wikipedia GDP-by-country
table.

> Inspired by a project from the IBM Data Engineering Professional Certificate. The original was a
> local script writing to a CSV file and SQLite. This version was rebuilt for the cloud:
> parameterised, orchestrated, idempotent, and secured with managed identity instead of hardcoded
> paths and keys.

---

## Architecture

```mermaid
flowchart LR
    W[Data source: website<br/>e.g. GDP by country table] --> E

    subgraph JOB["Databricks Job"]
        E[1 · extract] --> T[2 · transform] --> L[3 · load]
    end

    subgraph LAKE["ADLS Gen2 + Unity Catalog"]
        B[(bronze<br/>raw HTML + logs)]
        S[(silver<br/>cleaned table<br/>e.g. countries_gdp)]
        G[(gold<br/>current snapshot + CSV export)]
    end

    R[/Rate conversion file<br/>exchange_rate.csv/] --> T
    E --> B
    B --> T
    T --> S
    S --> L
    L --> G
    L --> N[(Database<br/>e.g. Neon Postgres)]
```

Tasks hand off through storage rather than memory, so each stage can be re-run on its own. A parsing
fix can be replayed against a saved page without scraping the site again.

| Task | Does | Writes to |
|---|---|---|
| **extract** | Fetches the page. Nothing is parsed here. | Bronze: `page_<run_date>.html` |
| **transform** | Finds the table by column keywords, parses values and years, applies the exchange rate | Silver: one snapshot per run date |
| **load** | Publishes the current snapshot | Gold table + CSV export, and the database |

---

## Tech stack

Azure Databricks (Serverless) · Unity Catalog · ADLS Gen2 · Delta Lake · Databricks Jobs ·
Neon Serverless Postgres · Python (`requests`, `BeautifulSoup`, `pandas`, `PySpark`, `SQLAlchemy`)

---

## Repository structure

```
├── src/
│   ├── 01_extract.py      # website  -> bronze
│   ├── 02_transform.py    # bronze   -> silver
│   └── 03_load.py         # silver   -> gold + database
├── notebooks/
│   └── 01_explore_table.py   # profiling work that shaped the parsing logic
└── README.md
```

Files are in Databricks notebook source format: valid `.py` for version control, importable as
notebooks for interactive work.

---

## Configuration

Job parameters, shown with this project's defaults:

| Parameter | Example | Purpose |
|---|---|---|
| `source_url` | Wikipedia GDP (nominal) | Page to scrape |
| `table_keywords` | `Country,IMF,World Bank` | Identifies the target table by its header |
| `target_currency` | `USD` | Looked up in the rate conversion file; USD skips conversion |
| `value_scale` | `0.001` | Source units to output units (millions to billions) |
| `run_date` | `{{job.start_time.iso_date}}` | Shared by all tasks so the handoff stays aligned |
| `catalog`, `silver_table`, `gold_table`, `pg_table` | `gdp_etl_catalog`, `countries_gdp`, … | Destinations |
| `allow_schema_change` | `false` | Guard against rebuilding a table when columns change |

The rate used is stored on every row, so each snapshot records how it was produced.

---

## Design decisions

* **No credentials in code.** Storage is reached through a managed identity with least-privilege access, and the database password lives in a Unity Catalog secret.
* **Idempotent.** Re-running a date replaces it instead of duplicating it, which matters because scheduled jobs get retried.
* **History, not overwrite.** Silver keeps one timestamped snapshot per run date, so estimate revisions can be tracked over time.
* **Source-agnostic parsing.** The table is found by column keywords and the schema is built from the page's own header. A mismatch with the existing table stops the run with a clear message.

---

## What the exploration found

Profiling the source table before rewriting the parser exposed a silent bug: some cells carry their
own estimate year (a value followed by `(2025)` where the header says 2026). Coercing those to
numbers turned them into `NaN`, so dozens of values were **dropped without any error**.

The parser now splits each cell into a value and a year, so those rows are retained and correctly
dated.

---

## Where it can break

Scraping means depending on a page that nobody promised to keep stable, so the honest list of
weaknesses matters as much as the happy path.

* **A layout change can quietly shrink the data.** Rows whose cell count does not match the header are skipped. If the source adds a merged header or an extra column, the run can still succeed while loading far fewer countries than it should. A minimum row threshold would turn that into a failure instead of a bad snapshot.
* **Unrecognised number formats become nulls.** Anything the parser cannot read is stored as empty rather than raising. That is deliberate, since real tables have missing values, but it means a new format would silently look like missing data.
* **The exchange rate is a static file.** Nothing checks how old it is or where it came from, so a stale rate produces confident, wrong numbers. The rate is stored on each row, which at least makes it auditable after the fact.
* **A renamed source column changes the schema.** Column names come from the page header, so renaming a source breaks the match with the existing table. The run stops with an explanation instead of corrupting the table, but it needs a person to decide what happens next.
* **The load is not atomic across destinations.** Gold and the CSV are written before the database load. If the database step fails, the lakehouse is ahead of Postgres until the run is retried.
* **Intra-day reruns overwrite each other.** History is tracked per date, so two runs on the same day leave only the later one.
* **Scale.** Parsing and conversion happen in pandas on a single node, which is right for hundreds of rows and wrong for millions.
* **Credentials expire.** A rotated database password or an expiring secret stops the load task until it is updated.

Next steps I would take: a row-count and null-rate threshold that fails the run, exchange rates
pulled from an API with an as-of date, tests against a saved HTML fixture so they never touch the
network, and a gold view comparing snapshots to surface revisions.

---

## Running it

Needs a Databricks workspace with Unity Catalog, an ADLS Gen2 account, and a Postgres database.
Create the catalog and medallion schemas, grant the Access Connector access to storage, upload the
rate conversion file, store the database password as a secret, then chain the three files as
`extract → transform → load` in a job with the parameters above.
