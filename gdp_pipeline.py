# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — Extract
# MAGIC Fetches the source page and saves it, untouched, into the bronze volume as `page_<run_date>.html`.
# MAGIC
# MAGIC Nothing is parsed here. Keeping the raw page means the transform task can be re-run or fixed
# MAGIC later without scraping the site again.
# MAGIC
# MAGIC **Re-running the same `run_date` overwrites that date's file**, so the task can be repeated safely.

# COMMAND ----------

# ============================================================
# 1. PARAMETERS
# ============================================================

dbutils.widgets.text("catalog", "gdp_etl_catalog")
dbutils.widgets.text("run_date", "")          # set by the job; blank = today (UTC)
dbutils.widgets.text("source_url", "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)")

CATALOG    = dbutils.widgets.get("catalog")
SOURCE_URL = dbutils.widgets.get("source_url")

BRONZE_DIR = f"/Volumes/{CATALOG}/bronze/raw_html"
REQUEST_HEADERS = {"User-Agent": "GDP-ETL-Portfolio/1.0"}

# COMMAND ----------

# ============================================================
# 2. IMPORTS
# ============================================================

import os
from datetime import date, datetime, timezone

import requests

RUN_DATE = date.fromisoformat(dbutils.widgets.get("run_date")) if dbutils.widgets.get("run_date") \
    else datetime.now(timezone.utc).date()

STAGE = "extract"

# COMMAND ----------

# ============================================================
# 3. FUNCTIONS
# ============================================================

LOG_LINES = []


def write_file(path, content):
    """Write a text file to a volume, replacing anything already at that path.

    Volumes don't allow appending to or editing a file in place, so a re-run has to
    delete first. Without this, the second run of a date fails with 'Illegal seek'.
    """
    if os.path.exists(path):
        os.remove(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def log_progress(message):
    """Collect a timestamped message and print it. Returns nothing."""
    line = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} : [{STAGE}] {message}"
    LOG_LINES.append(line)
    print(line)


def write_log(log_dir, run_date, stage):
    """Save this task's collected log lines as one file."""
    path = f"{log_dir}/etl_log_{run_date}_{stage}.txt"
    write_file(path, "\n".join(LOG_LINES) + "\n")
    print(f"Log written to {path}")

# COMMAND ----------

# ============================================================
# 4. TASK EXECUTION
# ============================================================

log_progress(f"Initiating extraction (run_date {RUN_DATE})")

response = requests.get(SOURCE_URL, headers=REQUEST_HEADERS, timeout=30)
response.raise_for_status()
log_progress(f"Page fetched from {SOURCE_URL} ({len(response.text)} characters)")

raw_path = f"{BRONZE_DIR}/page_{RUN_DATE}.html"
write_file(raw_path, response.text)
log_progress(f"Raw page saved to {raw_path}")

log_progress("Extraction complete")
write_log(BRONZE_DIR, RUN_DATE, STAGE)


# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Transform
# MAGIC Reads the HTML saved by the extract task, finds the target table by column keywords,
# MAGIC parses the values (splitting out any embedded year), converts them into the target currency
# MAGIC using `exchange_rate.csv`, and writes the result to the silver Delta table.
# MAGIC
# MAGIC Output columns are built from whatever the scraped table contains, so a different source
# MAGIC page works without code changes.
# MAGIC
# MAGIC **Re-runs:** the same `run_date` replaces that date's rows rather than adding duplicates.
# MAGIC If the scraped columns no longer match the existing table, the task stops and explains why;
# MAGIC set `allow_schema_change` to `true` to rebuild the table from scratch.

# COMMAND ----------

# MAGIC %pip install beautifulsoup4 --quiet

# COMMAND ----------

# ============================================================
# 1. PARAMETERS
# ============================================================

dbutils.widgets.text("catalog", "gdp_etl_catalog")
dbutils.widgets.text("run_date", "")                               # set by the job; blank = today (UTC)
dbutils.widgets.text("table_keywords", "Country,IMF,World Bank")   # identifies the target table
dbutils.widgets.text("target_currency", "USD")                     # USD = no conversion
dbutils.widgets.text("value_scale", "0.001")                       # source millions -> billions
dbutils.widgets.text("rates_csv", "exchange_rate.csv")             # file name inside the bronze volume
dbutils.widgets.text("silver_table", "countries_gdp")
dbutils.widgets.text("allow_schema_change", "false")               # true = rebuild table if columns differ

CATALOG             = dbutils.widgets.get("catalog")
TABLE_KEYWORDS      = [k.strip().lower() for k in dbutils.widgets.get("table_keywords").split(",") if k.strip()]
TARGET_CURRENCY     = dbutils.widgets.get("target_currency").strip().upper()
VALUE_SCALE         = float(dbutils.widgets.get("value_scale"))
ALLOW_SCHEMA_CHANGE = dbutils.widgets.get("allow_schema_change").strip().lower() == "true"

BRONZE_DIR   = f"/Volumes/{CATALOG}/bronze/raw_html"
RATES_CSV    = f"{BRONZE_DIR}/{dbutils.widgets.get('rates_csv')}"
SILVER_TABLE = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"

# COMMAND ----------

# ============================================================
# 2. IMPORTS
# ============================================================

import os
import re
from datetime import date, datetime, timezone

import pandas as pd
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession, types as T

spark = SparkSession.builder.getOrCreate()

RUN_TS = datetime.now(timezone.utc).replace(microsecond=0)
RUN_DATE = date.fromisoformat(dbutils.widgets.get("run_date")) if dbutils.widgets.get("run_date") \
    else RUN_TS.date()

STAGE = "transform"

# COMMAND ----------

# ============================================================
# 3. FUNCTIONS
# ============================================================

LOG_LINES = []
VALUE_PATTERN = re.compile(r"^([\d,]+(?:\.\d+)?)\s*(?:\(\s*(\d{4})\s*\))?$")


def write_file(path, content):
    """Write a text file to a volume, replacing anything already at that path.

    Volumes don't allow appending or editing in place, so a re-run has to delete first.
    """
    if os.path.exists(path):
        os.remove(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def log_progress(message):
    """Collect a timestamped message and print it. Returns nothing."""
    line = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} : [{STAGE}] {message}"
    LOG_LINES.append(line)
    print(line)


def write_log(log_dir, run_date, stage):
    """Save this task's collected log lines as one file."""
    path = f"{log_dir}/etl_log_{run_date}_{stage}.txt"
    write_file(path, "\n".join(LOG_LINES) + "\n")
    print(f"Log written to {path}")


def get_rate(rates_csv, currency):
    """Look up the conversion rate for one currency. USD is the source currency, so it needs none."""
    if currency == "USD":
        return 1.0
    rates = pd.read_csv(rates_csv)
    rates.columns = [c.strip().lower() for c in rates.columns]
    match = rates.loc[rates["currency"].str.strip().str.upper() == currency, "rate"]
    if match.empty:
        raise ValueError(f"{currency} not found in {rates_csv}")
    return float(match.iloc[0])


def find_table(html, keywords):
    """Return the table whose header row contains every keyword.

    Chosen by column names rather than position or caption, so the same task works
    on a different page as long as the keywords describe its header.
    """
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        for sup in table.find_all("sup"):          # drop footnote markers such as [n 1]
            sup.decompose()
        rows = table.find_all("tr")
        if not rows:
            continue
        header = " ".join(c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])).lower()
        if all(k in header for k in keywords):
            return table
    raise ValueError(f"No table found whose header contains all of: {keywords}")


def extract_rows(table):
    """Turn the table into a DataFrame of raw strings, using its own column names."""
    rows = table.find_all("tr")
    header = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
    records = [
        [c.get_text(" ", strip=True) for c in row.find_all("td")]
        for row in rows
        if len(row.find_all("td")) == len(header)
    ]
    return pd.DataFrame(records, columns=header)


def transform(raw_df, rate, currency, scale, run_date, run_ts):
    """Clean every value column: strip commas, split out an embedded year, apply scale and rate.

    '32,383,920' -> value with the column's header year; '407,786 (2025)' -> value with year 2025.
    """
    def clean_name(name):
        name = re.sub(r"\(.*?\)|\[.*?\]", "", name)            # remove '(2025)' and '[1]'
        return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

    def parse(cell, default_year):
        match = VALUE_PATTERN.match(str(cell).strip())
        if not match:
            return None, None                                  # '— N/a' and anything unparseable
        value = float(match.group(1).replace(",", ""))
        return value, int(match.group(2)) if match.group(2) else default_year

    df = raw_df[raw_df.iloc[:, 0] != "World"]                  # drop the aggregate row
    out = pd.DataFrame({"country": df.iloc[:, 0].str.strip()})

    for column in raw_df.columns[1:]:
        name = clean_name(column)
        header_year = re.search(r"(\d{4})", column)
        header_year = int(header_year.group(1)) if header_year else None

        parsed = [parse(cell, header_year) for cell in df[column]]
        out[f"{name}_value"] = [round(v * scale * rate, 2) if v is not None else None for v, _ in parsed]
        out[f"{name}_year"] = [y for _, y in parsed]

    out["currency"] = currency
    out["rate"] = rate
    out["run_date"] = run_date
    out["extracted_at"] = run_ts
    return out.reset_index(drop=True)


def to_spark(df):
    """Convert the pandas DataFrame to Spark, typing columns by their name suffix.

    Building the schema this way keeps the task source-agnostic: whatever columns the
    page produced become _value doubles and _year integers.
    """
    fields = []
    for column in df.columns:
        if column.endswith("_value") or column == "rate":
            fields.append(T.StructField(column, T.DoubleType()))
        elif column.endswith("_year"):
            fields.append(T.StructField(column, T.IntegerType()))
        elif column == "run_date":
            fields.append(T.StructField(column, T.DateType()))
        elif column == "extracted_at":
            fields.append(T.StructField(column, T.TimestampType()))
        else:
            fields.append(T.StructField(column, T.StringType()))

    rows = [
        tuple(None if pd.isna(value) else value for value in record)
        for record in df.itertuples(index=False, name=None)
    ]
    return spark.createDataFrame(rows, T.StructType(fields))


def write_silver(sdf, table, run_date, allow_schema_change):
    """Write this run's snapshot to Delta and return what the write did.

    Normal case: replace only this run_date's rows, so re-running a date is safe and
    earlier snapshots are kept. If the scraped columns differ from the existing table,
    Delta cannot merge them (and schema overwrite is not allowed alongside replaceWhere),
    so the task either stops with an explanation or rebuilds the table on request.
    """
    if not spark.catalog.tableExists(table):
        sdf.write.saveAsTable(table)
        return "created"

    existing = spark.table(table).columns
    if existing != sdf.columns:
        if not allow_schema_change:
            raise ValueError(
                "Column mismatch with the existing table. Set allow_schema_change=true to rebuild it "
                "(this drops earlier snapshots), or point silver_table at a new name.\n"
                f"  existing : {existing}\n"
                f"  new      : {sdf.columns}"
            )
        sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        return "rebuilt (previous snapshots dropped)"

    (sdf.write.mode("overwrite")
        .option("replaceWhere", f"run_date = '{run_date}'")
        .saveAsTable(table))
    return "this run_date replaced"

# COMMAND ----------

# ============================================================
# 4. TASK EXECUTION
# ============================================================

log_progress(f"Initiating transformation (run_date {RUN_DATE})")

rate = get_rate(RATES_CSV, TARGET_CURRENCY)
log_progress(f"Exchange rate loaded: 1 USD = {rate} {TARGET_CURRENCY}")

raw_path = f"{BRONZE_DIR}/page_{RUN_DATE}.html"
with open(raw_path, "r", encoding="utf-8") as f:
    html = f.read()
log_progress(f"Raw page read from {raw_path}")

table = find_table(html, TABLE_KEYWORDS)
raw_df = extract_rows(table)
log_progress(f"Table parsed ({len(raw_df)} rows, columns: {list(raw_df.columns)})")

df = transform(raw_df, rate, TARGET_CURRENCY, VALUE_SCALE, RUN_DATE, RUN_TS)
log_progress(f"Transformation complete ({len(df)} rows, columns: {list(df.columns)})")
display(df.head(10))

outcome = write_silver(to_spark(df), SILVER_TABLE, RUN_DATE, ALLOW_SCHEMA_CHANGE)
log_progress(f"Data written to {SILVER_TABLE} ({outcome})")
write_log(BRONZE_DIR, RUN_DATE, STAGE)

# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3 — Load
# MAGIC Takes this run's silver snapshot and publishes it two ways:
# MAGIC
# MAGIC 1. **Gold** — a Delta table in the lakehouse, plus a dated CSV in the gold `exports` volume
# MAGIC 2. **Database** — the Neon Postgres table
# MAGIC
# MAGIC **Re-runs:** the gold table and the CSV are replaced, and the Postgres rows for this
# MAGIC `run_date` are deleted before the insert, so nothing is duplicated. If the columns no longer
# MAGIC match the existing Postgres table, the task stops and explains why; set `allow_schema_change`
# MAGIC to `true` to rebuild that table.

# COMMAND ----------

# MAGIC %pip install sqlalchemy psycopg2-binary --quiet

# COMMAND ----------

# ============================================================
# 1. PARAMETERS
# ============================================================
%pip install sqlalchemy
dbutils.widgets.text("catalog", "gdp_etl_catalog")
dbutils.widgets.text("run_date", "")                  # set by the job; blank = today (UTC)
dbutils.widgets.text("silver_table", "countries_gdp")
dbutils.widgets.text("gold_table", "countries_gdp")
dbutils.widgets.text("pg_host", "ep-twilight-sound-ayi2ejh4.c-5.us-east-2.aws.neon.tech")
dbutils.widgets.text("pg_database", "GDP_ETL_DB")
dbutils.widgets.text("pg_user", "neondb_owner")
dbutils.widgets.text("pg_table", "countries_by_gdp")
dbutils.widgets.text("secret_schema", "bronze")       # schema holding the 'neon_password' secret
dbutils.widgets.text("allow_schema_change", "false")  # true = rebuild the Postgres table if columns differ

CATALOG             = dbutils.widgets.get("catalog")
SILVER_TABLE        = f"{CATALOG}.silver.{dbutils.widgets.get('silver_table')}"
GOLD_TABLE          = f"{CATALOG}.gold.{dbutils.widgets.get('gold_table')}"
EXPORT_DIR          = f"/Volumes/{CATALOG}/gold/exports"
BRONZE_DIR          = f"/Volumes/{CATALOG}/bronze/raw_html"

PG_HOST             = dbutils.widgets.get("pg_host")
PG_DATABASE         = dbutils.widgets.get("pg_database")
PG_USER             = dbutils.widgets.get("pg_user")
PG_TABLE            = dbutils.widgets.get("pg_table")
SECRET_SCHEMA       = dbutils.widgets.get("secret_schema")
ALLOW_SCHEMA_CHANGE = dbutils.widgets.get("allow_schema_change").strip().lower() == "true"

# COMMAND ----------

# ============================================================
# 2. IMPORTS
# ============================================================

import os
from datetime import date, datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

RUN_DATE = date.fromisoformat(dbutils.widgets.get("run_date")) if dbutils.widgets.get("run_date") \
    else datetime.now(timezone.utc).date()

STAGE = "load"

# COMMAND ----------

# ============================================================
# 3. FUNCTIONS
# ============================================================

LOG_LINES = []


def remove_if_exists(path):
    """Volumes don't allow overwriting a file in place, so a re-run deletes it first."""
    if os.path.exists(path):
        os.remove(path)


def log_progress(message):
    """Collect a timestamped message and print it. Returns nothing."""
    line = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} : [{STAGE}] {message}"
    LOG_LINES.append(line)
    print(line)


def write_log(log_dir, run_date, stage):
    """Save this task's collected log lines as one file."""
    path = f"{log_dir}/etl_log_{run_date}_{stage}.txt"
    remove_if_exists(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES) + "\n")
    print(f"Log written to {path}")


def load_to_db(df, engine, table, run_date, allow_schema_change):
    """Load the snapshot into Postgres and return what the write did.

    Deleting this run_date's rows first is what makes a re-run safe. If the scraped
    columns no longer match the existing table, an append would fail, so the task
    either stops with an explanation or drops and rebuilds the table on request.
    """
    outcome = "created"
    with engine.begin() as conn:
        exists = conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar()
        if exists:
            existing = [
                row[0] for row in conn.execute(
                    text("SELECT column_name FROM information_schema.columns "
                         "WHERE table_name = :t ORDER BY ordinal_position"),
                    {"t": table},
                )
            ]
            if existing != list(df.columns):
                if not allow_schema_change:
                    raise ValueError(
                        "Column mismatch with the existing Postgres table. Set allow_schema_change=true "
                        "to drop and rebuild it, or point pg_table at a new name.\n"
                        f"  existing : {existing}\n"
                        f"  new      : {list(df.columns)}"
                    )
                conn.execute(text(f'DROP TABLE "{table}"'))
                outcome = "rebuilt (previous snapshots dropped)"
            else:
                conn.execute(text(f'DELETE FROM "{table}" WHERE run_date = :d'), {"d": run_date})
                outcome = "this run_date replaced"

    df.to_sql(table, engine, if_exists="append", index=False)
    return outcome

# COMMAND ----------

# ============================================================
# 4. TASK EXECUTION
# ============================================================

log_progress(f"Initiating load (run_date {RUN_DATE})")

# --- Gold: this run's snapshot, kept in the lakehouse ---------------
# CREATE OR REPLACE rebuilds the table each run, so re-running is safe by construction.
spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_TABLE} AS
    SELECT * FROM {SILVER_TABLE} WHERE run_date = '{RUN_DATE}'
""")
df = spark.table(GOLD_TABLE).toPandas()
if df.empty:
    raise ValueError(f"No rows in {SILVER_TABLE} for run_date {RUN_DATE}")
log_progress(f"Gold table refreshed: {GOLD_TABLE} ({len(df)} rows)")

# --- Gold: the shareable file ---------------------------------------
currency = df["currency"].iloc[0]
csv_path = f"{EXPORT_DIR}/gdp_{RUN_DATE}_{currency}.csv"
remove_if_exists(csv_path)
df.to_csv(csv_path, index=False)
log_progress(f"CSV exported to {csv_path}")

# --- Database: Neon Postgres ----------------------------------------
password = dbutils.secrets.get(catalog=CATALOG, schema=SECRET_SCHEMA, key="neon_password")
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{quote_plus(password)}@{PG_HOST}/{PG_DATABASE}?sslmode=require"
)
log_progress(f"Database connection initiated ({PG_HOST}/{PG_DATABASE})")

outcome = load_to_db(df, engine, PG_TABLE, RUN_DATE, ALLOW_SCHEMA_CHANGE)
log_progress(f"Data loaded into {PG_TABLE} ({outcome})")

check = pd.read_sql(
    f'SELECT COUNT(*) AS rows, COUNT(DISTINCT run_date) AS snapshots FROM "{PG_TABLE}"', engine
)
print(check)
log_progress(f"Verification: {int(check.iloc[0, 0])} rows across {int(check.iloc[0, 1])} snapshot(s)")

engine.dispose()
log_progress("Database connection closed")
log_progress("Load complete")
write_log(BRONZE_DIR, RUN_DATE, STAGE)


