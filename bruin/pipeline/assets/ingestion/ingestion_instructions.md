# Ingestion Instructions for LipidMythBuster

## The Big Picture

Right now, your data lives on the internet (Figshare) as an Excel file. Your goal is to bring it into your local DuckDB warehouse so Bruin can orchestrate transformations on it.

Think of it like this:

```
  FIGSHARE (internet)          YOUR PIPELINE (local)                    DuckDB (warehouse)
  ┌──────────────────┐         ┌────────────────────────────────┐      ┌──────────────────────────┐
  │  .xlsx file      │ ──GET──>│  Python asset                  │──LOAD>│  raw table               │
  │  ~1,688 rows     │         │  (cac_health_screening.py)     │      │  cac_health_screening    │
  │  Korean CAC data │         │  download + parse + return df  │      │  ~1,688 rows in DuckDB   │
  └──────────────────┘         └────────────────────────────────┘      └──────────────────────────┘
```

You are building the middle piece: the Python ingestion asset.

---

## What You're Ingesting

**Source:** `https://ndownloader.figshare.com/files/14624306`
**Format:** Excel (.xlsx) -- single file, ~198 KB
**Rows:** ~1,688 (Korean asymptomatic health screening participants)
**Published by:** SU-YEON CHOI (2019), CC BY 4.0

Since the exact column names are inside the Excel file, your first task before writing code is to **download the file manually and inspect it** to discover the columns. You can do this quickly:

```bash
# Download it once to see what's inside
pip install openpyxl pandas
python -c "
import pandas as pd
df = pd.read_excel('https://ndownloader.figshare.com/files/14624306')
print(df.columns.tolist())
print(df.dtypes)
print(df.head())
print(f'Shape: {df.shape}')
"
```

Run that snippet. Write down:
1. Every column name exactly as it appears
2. The data type of each column (int, float, string)
3. Which column(s) could serve as a primary key (e.g., a subject ID)

You'll need those for Step 3 below.

---

## Step-by-Step Plan

### Step 1: Clean up the template files

The old taxi templates are causing validation errors. You need to clean them up.

**Delete these files** (they're taxi-specific and just noise):
- `bruin/pipeline/assets/ingestion/trips.py`
- `bruin/pipeline/assets/ingestion/payment_lookup.asset.yml`
- `bruin/pipeline/assets/ingestion/payment_lookup.csv`
- `bruin/pipeline/assets/staging/trips.sql`
- `bruin/pipeline/assets/reports/trips_report.sql`

**Also delete** the failed Grok approach (it used a non-existent `type: ingest`):
- `bruin/assets/ingestion/clinical_cac_raw.ingest.yml`

**Then create** your new Python ingestion file:
- `bruin/pipeline/assets/ingestion/cac_health_screening.py`

### Step 2: Update `pipeline.yml`

Open `bruin/pipeline/pipeline.yml` and **replace the entire file** with a clean version (no TODOs):

| Field | What to set | Why |
|-------|-------------|-----|
| `name` | `lipid_myth_buster` | Identifies your pipeline in logs |
| `schedule` | `daily` (or `weekly`) | How often Bruin should run. For a static dataset, `daily` is fine -- it won't re-download if you design it right |
| `start_date` | `"2019-03-16"` | The dataset publication date. This is the earliest date Bruin considers for backfills |
| `default_connections.duckdb` | `duckdb-default` | Tells all assets to use your local DuckDB by default |

**Delete the `variables` block** (the `taxi_types` stuff). You don't have taxi types -- your dataset is a single static file, not partitioned by type.

Your final `pipeline.yml` should look roughly like this:

```yaml
name: lipid_myth_buster
schedule: daily
start_date: "2019-03-16"

default_connections:
  duckdb: duckdb-default
```

That's it. Clean and simple.

### Step 3: Write the Python ingestion asset

This is the core of the ingestion. Here's what your new `.py` file needs:

#### A) The Bruin header (the docstring at the top)

Replace the entire `"""@bruin ... @bruin"""` block. Here's the structure:

```
"""@bruin
name: ingestion.cac_health_screening
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace       <-- NOT "append"!

columns:
  - name: <col1_from_step_0>
    type: <type>
    description: "<what it is>"
  - name: <col2_from_step_0>
    type: <type>
    ...
@bruin"""
```

**Why `create+replace` instead of `append`?**

This is important to understand:

```
APPEND strategy:                         CREATE+REPLACE strategy:
┌────────────────────┐                   ┌────────────────────┐
│ Run 1: 1688 rows   │                   │ Run 1: 1688 rows   │
│ Run 2: +1688 rows  │  = 3376 rows!     │ Run 2: 1688 rows   │  = 1688 rows
│ Run 3: +1688 rows  │  = 5064 rows!!    │ Run 3: 1688 rows   │  = 1688 rows
└────────────────────┘  DUPLICATES!      └────────────────────┘  CLEAN
```

- `append` adds rows every run. Great for streaming/incremental data (like taxi trips arriving daily).
- `create+replace` drops and recreates the table each run. Perfect for **static datasets** like yours that don't change.

Your Korean CAC dataset is a fixed snapshot -- it won't get new rows. So `create+replace` is the right choice.

**All valid Bruin strategies** (for future reference):
`create+replace`, `delete+insert`, `truncate+insert`, `append`, `merge`, `time_interval`, `ddl`, `scd2_by_time`, `scd2_by_column`

#### B) The `materialize()` function

This is where you write the actual Python logic. It's simple for a static file:

```
1. Download the Excel file from the URL
2. Parse it into a pandas DataFrame
3. (Optional) Add an `extracted_at` timestamp column for lineage
4. Return the DataFrame
```

That's it. Bruin takes the returned DataFrame and loads it into DuckDB for you. You don't write SQL, you don't manage connections -- Bruin handles all of that.

**Pseudocode** (not the final code, but the logic):

```
def materialize():
    url = "https://ndownloader.figshare.com/files/14624306"
    df = download_and_read_excel(url)
    df["extracted_at"] = current_timestamp()
    return df
```

#### C) The `requirements.txt`

Open `bruin/pipeline/assets/ingestion/requirements.txt` and add:

```
pandas
openpyxl
requests
```

`openpyxl` is what pandas needs under the hood to read `.xlsx` files. Without it, `pd.read_excel()` will fail.

### Step 4: Fix `.bruin.yml` (root config)

Your `.bruin.yml` was overwritten with Grok's format (flat `connections:` block) which
caused the "environment 'default' not found" error. Bruin requires the `environments` wrapper.

**Replace the entire file** with:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: ./lipid_myth_buster.duckdb
```

**Why this structure?** Bruin supports multiple environments (dev, staging, prod).
The `default_environment` key tells it which one to use when you don't specify.
Each environment has its own set of connections. The error you got earlier happened
because Grok's format skipped this wrapper entirely.

This tells Bruin: "when an asset says `connection: duckdb-default`, connect to this local DuckDB file."

### Step 5: Validate and Run

```bash
# Validate your pipeline (checks YAML, column defs, dependencies)
bruin validate bruin/pipeline

# Run just the ingestion asset
bruin run --asset ingestion.cac_health_screening bruin/pipeline

# Or run the full pipeline
bruin run bruin/pipeline
```

After running, you can verify the data landed:

```bash
# Quick check with DuckDB CLI (if installed)
duckdb lipid_myth_buster.duckdb "SELECT count(*) FROM cac_health_screening"
```

---

## Summary Checklist

```
[ ] 1. Download & inspect the Excel file (discover columns + types)
[ ] 2. Clean up: delete taxi templates AND the failed .ingest.yml from Grok
         - bruin/pipeline/assets/ingestion/trips.py
         - bruin/pipeline/assets/ingestion/payment_lookup.asset.yml
         - bruin/pipeline/assets/ingestion/payment_lookup.csv
         - bruin/pipeline/assets/staging/trips.sql
         - bruin/pipeline/assets/reports/trips_report.sql
         - bruin/assets/ingestion/clinical_cac_raw.ingest.yml
[ ] 3. Create your Python asset file (cac_health_screening.py)
         [ ] Write the @bruin header with correct columns
         [ ] Use strategy: create+replace
         [ ] Implement materialize() to download + return DataFrame
         [ ] IMPORTANT: clean column names (replace dots/hyphens with underscores)
[ ] 4. Fill in requirements.txt (pandas, openpyxl, requests)
[ ] 5. Replace pipeline.yml with clean version (no TODOs)
[ ] 6. Fix .bruin.yml (use environments wrapper, not flat connections)
[ ] 7. bruin validate --> fix any errors
[ ] 8. bruin run --> check data in DuckDB
```

---

## Common Pitfalls

| Pitfall | What happens | Fix |
|---------|-------------|-----|
| Missing `openpyxl` in requirements.txt | `pd.read_excel()` throws "Missing optional dependency 'openpyxl'" | Add `openpyxl` to requirements.txt |
| Using `append` strategy | Duplicate rows every time pipeline runs | Use `create+replace` for static datasets |
| Column names with spaces/special chars | DuckDB may complain or Bruin column checks fail | Rename columns in your DataFrame (e.g., `df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')`) |
| Empty `.bruin.yml` | Bruin can't find the DuckDB connection | Fill it with the connection config from Step 4 |
| Forgetting `image: python:3.11` | Bruin doesn't know which Python to use | Always set the image in the header |
