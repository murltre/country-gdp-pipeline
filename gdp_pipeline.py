
"""
ETL pipeline: Country GDP data from Wikipedia (IMF / World Bank / UN estimates)

Extracts the 'GDP by country' table, converts values from millions to billions
USD, and loads the result into both a CSV file and a SQLite database.
"""

# ============================================================
# 1. IMPORTS
# ============================================================

import os
import sqlite3
from datetime import datetime

import pandas as pd
import requests as r
from bs4 import BeautifulSoup as BS


# ============================================================
# 2. CONFIGURATION
# ============================================================

URL = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# Wikipedia rejects generic script user-agents, so identify the client.
REQUEST_HEADERS = {
    'User-Agent': 'GDP-ETL-Learning-Project/1.0 (meinseydouday120@gmail.com)'
}

DEST_PATH = r'C:\Users\HP\OneDrive\LEARNING_area\CERTIFICATES Course\ALL - IBM Data Engineering\Python Project for Data Engineering'

CSV_FILE = os.path.join(DEST_PATH, 'GDP_data.csv')
DB_FILE = os.path.join(DEST_PATH, 'GDP_data.db')
LOG_FILE = os.path.join(DEST_PATH, 'etl_project_log.txt')

TABLE_NAME = 'Countries_by_GDP'

# Final column names, applied after the millions -> billions conversion.
FINAL_COLUMNS = ['Country', 'IMF_USD_bn', 'World_Bank_USD_bn', 'UN_USD_bn']


# ============================================================
# 3. FUNCTIONS
# ============================================================

def log_progress(message, log_file=LOG_FILE):
    """Append a timestamped message to the log file. Returns nothing."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def fetch_page(url, request_headers):
    """Download the page and return it as a BeautifulSoup object.

    Raises if the request fails, so the pipeline stops at the point of
    failure rather than parsing an error page further downstream.
    """
    resp = r.get(url, headers=request_headers)
    if resp.status_code != 200:
        log_progress(f'Request failed with status {resp.status_code}')
        raise Exception(f'Request failed: {resp.status_code}')
    return BS(resp.text, 'html.parser')


def find_target_table(soup):
    """Return the 'GDP by country' wikitable.

    Selected by caption text rather than position, so the pipeline survives
    tables being added or reordered elsewhere on the page. Note the page also
    has a 'by region or grouping' table that must NOT be matched.
    """
    for table in soup.find_all('table', class_='wikitable'):
        caption = table.find('caption')
        if caption and 'by country' in caption.get_text():
            return table
    return None


def extract(target_table, column_names=None):
    """Scrape the table into a DataFrame of raw strings.

    No cleaning happens here - extract captures the page faithfully and
    leaves conversion to transform().
    """
    rows = target_table.find_all('tr')

    # Derive column names from the header row unless supplied.
    if column_names is None:
        header_cells = rows[0].find_all(['th', 'td'])
        column_names = [c.get_text(' ', strip=True) for c in header_cells]

    records = []
    for row in rows:
        cells = row.find_all('td')

        # Header rows have 0 <td>, so this also guards the indexing below.
        if len(cells) != len(column_names):
            continue
        # The 'World' aggregate row has no link in its first cell.
        if cells[0].find('a') is None:
            continue

        values = []
        for cell in cells:
            # Prefer link text: it strips footnote markers, e.g. 'China[n 1]'.
            link = cell.find('a')
            values.append(link.get_text(strip=True) if link
                          else cell.get_text(strip=True))

        records.append(dict(zip(column_names, values)))

    return pd.DataFrame(records, columns=column_names)


def transform(df):
    """Clean the numeric columns and convert millions -> billions USD."""
    df = df.copy()

    # Everything except the first (country) column holds GDP figures.
    value_cols = [c for c in df.columns if c != df.columns[0]]

    for col in value_cols:
        cleaned = df[col].str.replace(',', '', regex=False)
        # errors='coerce' turns unparseable values such as '-N/a' into NaN
        # instead of raising, so rows with partial data are preserved.
        df[col] = pd.to_numeric(cleaned, errors='coerce')
        df[col] = (df[col] / 1000).round(2)

    df = df.sort_values(by=value_cols[0], ascending=False, na_position='last')
    df = df.reset_index(drop=True)

    return df


def load_to_csv(df, csv_file):
    """Save the DataFrame as a CSV file. Returns nothing."""
    df.to_csv(csv_file, index=False)


def load_to_db(df, sql_connection, table_name):
    """Save the DataFrame as a database table. Returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    """Run a query, print the statement and its output, and return the result."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print()
    return query_output


# ============================================================
# 4. PIPELINE EXECUTION
# ============================================================

log_progress('Preliminaries complete. Initiating ETL process')

soup = fetch_page(URL, REQUEST_HEADERS)
log_progress('Page fetched and parsed')

target_table = find_target_table(soup)
if target_table is None:
    log_progress('Target table not found')
    raise Exception('Target table not found')
log_progress('Target table identified')

df = extract(target_table)
log_progress(f'Data extraction complete ({len(df)} rows). Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

df.columns = FINAL_COLUMNS
log_progress('Columns renamed')

load_to_csv(df, CSV_FILE)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(DB_FILE)
log_progress('SQL Connection initiated')

load_to_db(df, conn, TABLE_NAME)
log_progress('Data loaded to Database as table. Running the queries')

# --- Query 1: countries with IMF GDP of at least 100 billion USD ---
run_query(
    f"SELECT * FROM {TABLE_NAME} WHERE IMF_USD_bn >= 100",
    conn
)

# --- Query 2: shape of the loaded table (row count and column count) ---
# COUNT(*) gives the rows; PRAGMA table_info returns one row per column,
# so its length is the column count.
row_count = run_query(
    f"SELECT COUNT(*) AS total_rows FROM {TABLE_NAME}",
    conn
).iloc[0, 0]

column_count = len(pd.read_sql(f"PRAGMA table_info({TABLE_NAME})", conn))

print(f"Loaded '{TABLE_NAME}': {row_count} rows x {column_count} columns")
log_progress(f'Verification: {row_count} rows and {column_count} columns in database')

conn.close()
log_progress('SQL Connection closed')
log_progress('Process Complete')


