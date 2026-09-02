# Wikipedia GDP Pipeline

An ETL pipeline that extracts country GDP figures from Wikipedia, converts them
from millions to billions of USD, and loads the result into both a CSV file and
a SQLite database.

The source table carries three independent estimates for each country — IMF,
World Bank, and United Nations — and all three are preserved through the
pipeline.

---

## Output

The pipeline produces `GDP_data.csv` and a `Countries_by_GDP` table in
`GDP_data.db`. First five rows:

| Country | IMF_USD_bn | World_Bank_USD_bn | UN_USD_bn |
|---|---|---|---|
| United States | 32383.92 | 30769.70 | 29298.00 |
| China | 20851.59 | 19498.04 | 18743.80 |
| Germany | 5452.86 | 5050.92 | 4659.93 |
| Japan | 4379.25 | 4435.16 | 4026.21 |
| United Kingdom | 4264.79 | 4002.59 | 3685.88 |

221 rows total, covering IMF members alongside non-sovereign territories and
states with limited recognition.

Every stage writes to `etl_project_log.txt`:

```
2026-09-02 20:14:07 : Preliminaries complete. Initiating ETL process
2026-09-02 20:14:09 : Page fetched and parsed
2026-09-02 20:14:09 : Target table identified
2026-09-02 20:14:09 : Data extraction complete (221 rows). Initiating Transformation process
2026-09-02 20:14:09 : Data transformation complete. Initiating loading process
```

---

## Running it

```bash
git clone https://github.com/murltre/wikipedia-gdp-pipeline.git
cd wikipedia-gdp-pipeline
pip install -r requirements.txt
python gdp_pipeline.py
```

Outputs are written next to the script. No configuration needed.

---

## How it works

| Stage | Function | What it does |
|---|---|---|
| Fetch | `fetch_page()` | Requests the page, raises on a non-200 response |
| Locate | `find_target_table()` | Finds the correct table by caption text |
| Extract | `extract()` | Parses rows into a DataFrame of raw strings |
| Transform | `transform()` | Cleans values, converts to billions, sorts |
| Load | `load_to_csv()`, `load_to_db()` | Writes to CSV and SQLite |
| Verify | `run_query()` | Reads back from the database to confirm the load |

Logging runs alongside every stage rather than inside the functions, so the
functions stay independently testable.

---

## Design notes

Four decisions where the obvious approach turned out to be wrong.

**Tables are located by caption, not by position.**
The page contains two `wikitable` elements: one listing individual countries
and one listing regional aggregates such as the EU and ASEAN. Selecting by
index would silently pull the wrong table if Wikipedia ever reorders the page,
and the resulting data would look plausible while being wrong. Matching on the
caption text `by country` targets meaning rather than layout.

**Requests must identify themselves.**
Wikipedia returns `403 Forbidden` to clients using the default
`python-requests` user-agent. The pipeline sends a descriptive `User-Agent`
header with contact details, which is what Wikipedia's automated-access policy
asks for. The status code is checked before parsing — without that check, an
error page flows silently into BeautifulSoup and the pipeline produces zero
rows instead of failing.

**Missing values become `NaN` rather than dropping the row.**
Some entries have no estimate from every source. Montserrat, for example, has
no IMF or World Bank figure but does have a UN one. Dropping incomplete rows
would discard real data, so `pd.to_numeric(..., errors='coerce')` converts
unparseable entries to `NaN` and the row survives with whatever data it has.

**Country names come from link text, not cell text.**
Several cells carry footnote markers inside them — the cell for China reads
`China[n 1]`. Reading the `<a>` element instead of the full cell returns the
clean name. The `World` total row is excluded by the same mechanism: it is the
only row whose first cell contains no link.

---

## Known limitations

- **The source is a scraped rendering, not the origin.** Wikipedia reproduces
  figures published by the IMF, World Bank, and UN. Reading those sources
  directly would be more robust; Wikipedia is used here because the table
  consolidates all three.
- **Estimate years are dropped.** The source headers carry the year for each
  estimate (`IMF (2026)`), but the output columns do not. Re-running after a
  Wikipedia update would produce different figures under identical column
  names, with nothing to distinguish them.
- **Each run replaces the previous output.** The table is written with
  `if_exists='replace'`, so no history accumulates. Storing an `as_of` date and
  appending would make the data a time series.

---

## Built with

Python · requests · BeautifulSoup · pandas · SQLite
