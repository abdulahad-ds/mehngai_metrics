# PBS Weekly Sensitive Price Index (SPI) Lakehouse Pipeline

A PySpark pipeline on Databricks that loads the weekly SPI annexures published by the Pakistan Bureau of Statistics (PBS) into a Bronze and Silver Lakehouse. Each weekly annexure holds the minimum, average and maximum price of 51 essential items in 17 cities, so a complete week has 867 rows (17 x 51).

Data source: https://www.pbs.gov.pk/price-statistics/ (Weekly Sensitive Price Index, "Annexure" files).

## Contents

1. [Architecture](#1-architecture)
2. [Repository layout](#2-repository-layout)
3. [Data models](#3-data-models)
4. [How the requirements are met](#4-how-the-requirements-are-met)
5. [Execution guide](#5-execution-guide)
6. [Verifying the pipeline](#6-verifying-the-pipeline)
7. [Known limitations](#7-known-limitations)

## 1. Architecture

```
PBS website
   |  pbs_spi_scraper.py (run on a PC, writes spi_weeks_flags.csv)
   v
spi_weeks_flags.csv  --upload-->  /Volumes/workspace/spi/raw/manifest/
   |  01_download_annexures
   v
/Volumes/workspace/spi/raw/xlsx   and   /Volumes/workspace/spi/raw/pdf
   |  01_raw_to_bronze        (Excel parser or PDF parser, all values kept as text)
   v
workspace.spi.bronze_spi_prices
   |  02_bronze_to_silver     (typed, cleaned, validated, MERGE)
   v
workspace.spi.silver_spi_prices        workspace.spi.silver_spi_quarantine (rejected rows)

Every step writes to workspace.spi.pipeline_execution_logs
```

Why the scraper runs on a PC: the table on the PBS page is filled in by JavaScript, so a browser is needed to read it, and the Databricks compute used for this project cannot install one. The scraper produces a CSV of annexure links (one row per week), and a Databricks notebook downloads the files listed in it.

Annexure selection rule: for each week, if an Excel annexure exists only the xlsx is saved, otherwise the pdf is saved. Links are chosen by file extension and file name (`annex` in the name), not by table column, because the website sometimes puts the xlsx in the pdf column or swaps the annexure and summary links. Links whose file name carries a different week's date are rejected.

## 2. Repository layout

| File | Purpose |
|---|---|
| `pbs_spi_scraper.py` | Runs on a PC with Playwright. Reads the SPI table and writes `spi_weeks_flags.csv`. |
| `00_config.py` | Shared settings: table names, explicit schemas, Excel and PDF parsers, `ensure_tables()`, `write_log()`. Loaded by the other notebooks with `%run ./00_config`. |
| `01_download_annexures.py` | Downloads the annexures listed in the CSV into the raw volume. |
| `01_raw_to_bronze.py` | Parses the xlsx and pdf files and merges them into Bronze. |
| `02_bronze_to_silver.py` | Casts, validates and merges Bronze into Silver, and quarantines bad rows. |
| `proof/` | Exported notebook outputs (HTML) showing the pipeline running. |

## 3. Data models

All tables are Delta tables in `workspace.spi`. Delta does not enforce primary keys, so each key below is enforced by the `MERGE INTO` join condition that loads the table.

### 3.1 Bronze: `bronze_spi_prices`

One row per source file, item and city. Every value from the file is kept as text, exactly as parsed. Only the two metadata columns are typed.

Primary key: (`source_file`, `item_sl_no`, `city_raw`)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `batch_id` | string | no | Identifier of the run that loaded the row. |
| `source_file` | string | no | Name of the annexure file, e.g. `Annex_24.09.2026.xlsx`. |
| `price_date_text` | string | yes | Price date as found in the file (`dd-MM-yyyy`), or taken from the file name if missing in the file. |
| `item_sl_no` | string | no | Serial number of the item (1 to 51), as text. |
| `item_description` | string | yes | Item name as written in the file. |
| `unit` | string | yes | Unit as written in the file, e.g. `20 Kg`. |
| `city_raw` | string | no | City with its code, e.g. `Islamabad (01)`. |
| `min_price_raw` | string | yes | Minimum price as text. |
| `avg_price_raw` | string | yes | Average price as text. |
| `max_price_raw` | string | yes | Maximum price as text. |
| `load_timestamp` | timestamp | no | When the row was ingested. |

### 3.2 Silver: `silver_spi_prices`

One row per week, city and item, with proper types.

Primary key: (`price_date`, `city_code`, `item_no`)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `price_date` | date | no | Date of the prices, cast from `price_date_text`. |
| `city_code` | int | no | City code taken from `city_raw`, e.g. 1. |
| `city_name` | string | yes | City name taken from `city_raw`, e.g. `Islamabad`. |
| `item_no` | int | no | Item serial number (1 to 51). |
| `item_name` | string | yes | One standard name per item (see below). |
| `unit` | string | yes | One standard unit per item (see below). |
| `min_price` | double | yes | Minimum price. NULL when not available. |
| `avg_price` | double | yes | Average price. NULL when not available. |
| `max_price` | double | yes | Maximum price. NULL when not available. |
| `is_available` | boolean | no | `false` when the item had no price in that city that week. |
| `source_file` | string | yes | Bronze file the row came from. |
| `batch_id` | string | yes | Identifier of the Silver run that wrote the row. |
| `load_timestamp` | timestamp | no | When the row was processed into Silver. |

Standard item names and units: the files word the same item differently over time (for example a trailing `*` on `Electricity Charges for Q1*`, or the Excel and PDF parsers splitting `Wheat Flour` / `Bag 20 Kg` differently). Silver takes one name and unit per `item_no` from its most recent week and strips trailing `*` and extra spaces, so each item has exactly one name and one unit.

### 3.3 Quarantine: `silver_spi_quarantine`

Bronze rows that could not be cast or failed validation, kept in their original text form with the reason.

Primary key: (`source_file`, `item_sl_no`, `city_raw`)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `batch_id` | string | yes | Run that rejected the row. |
| `source_file` | string | no | Source file. |
| `price_date_text` | string | yes | Raw date text. |
| `item_sl_no` | string | no | Raw item serial number. |
| `item_description` | string | yes | Raw item name. |
| `unit` | string | yes | Raw unit. |
| `city_raw` | string | no | Raw city text. |
| `min_price_raw` | string | yes | Raw minimum price. |
| `avg_price_raw` | string | yes | Raw average price. |
| `max_price_raw` | string | yes | Raw maximum price. |
| `reject_reason` | string | yes | Why the row was rejected. |
| `load_timestamp` | timestamp | no | When the row was quarantined. |

### 3.4 Audit log: `pipeline_execution_logs`

One row per processed unit of work.

Primary key: `log_id`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `log_id` | string | no | Unique id (UUID) of the log row. |
| `batch_id` | string | yes | Run identifier. |
| `layer` | string | no | `Raw-to-Bronze` or `Bronze-to-Silver`. |
| `run_mode` | string | yes | `full` or `incremental`. |
| `parameter` | string | yes | File name (Raw-to-Bronze) or the date range / source file filter (Bronze-to-Silver). |
| `start_time` | timestamp | yes | When processing started. |
| `end_time` | timestamp | yes | When processing ended. |
| `status` | string | yes | `Success` or `Failure`. |
| `rows_inserted` | bigint | yes | Rows inserted by the MERGE. |
| `rows_updated` | bigint | yes | Rows updated by the MERGE. |
| `rows_quarantined` | bigint | yes | Rows sent to quarantine. |
| `message` | string | yes | Warning or error text. |
| `load_timestamp` | timestamp | no | When the log row was written. |

## 4. How the requirements are met

### Explicit schemas, no inference

Every schema is defined with `StructType` and `StructField` in `00_config.py` (`BRONZE_PARSE_SCHEMA`, `BRONZE_SCHEMA`, `SILVER_SCHEMA`, `QUARANTINE_SCHEMA`, `LOG_SCHEMA`). Files are parsed by our own Excel and PDF parsers into tuples, and the DataFrame is created with the explicit schema. `inferSchema` is never used. `ensure_tables()` creates the tables from these schemas and is safe to run repeatedly.

### Typing and casting

Bronze keeps text. Silver converts with `try_cast` / `try_to_timestamp`: date text to `date`, item and city codes to `int`, prices to `double`. A value that is not valid becomes NULL instead of crashing the batch, and the row is quarantined with a reason.

Silver validation:

- Bad `price_date`, `city_code` or `item_no`: quarantined (`bad date`, `bad city code`, `bad item number`).
- A price that has text but is not a number: quarantined (`non-numeric price`).
- A negative price: quarantined (`negative price`).
- Minimum, average and maximum out of order: quarantined (`min/avg/max out of order`).
- A price written as `NA`, `N.A.`, `-`, `--` or empty is a normal missing value, not an error. The row is kept with NULL prices and `is_available = false`.

Missing prices are never dropped or filled in Bronze or Silver. Any imputation belongs in a later layer, where the method can be documented.

### Metadata

Every table has a `load_timestamp` column: Bronze, Silver, quarantine and the log table.

### Idempotency

Bronze, Silver and quarantine are all loaded with `MERGE INTO`:

- Bronze joins on (`source_file`, `item_sl_no`, `city_raw`).
- Silver joins on (`price_date`, `city_code`, `item_no`).
- Quarantine joins on (`source_file`, `item_sl_no`, `city_raw`).

A matched row is updated only if a value actually changed, otherwise it is left alone. Running the pipeline again on the same data therefore gives `rows_inserted = 0` and `rows_updated = 0`. If the same Silver key arrives from two files (for example an xlsx and a pdf of the same week), one row is kept before the MERGE, because Delta rejects several source rows for one target row.

### Parameterized runs and backfills

No notebook contains a hardcoded date or file path. Folders, date ranges, file names, run mode and batch id are all widgets (see the execution guide). Any historical period can be re-run from raw files to Bronze, or from Bronze to Silver.

### Schema drift

- Raw to Bronze: if a file has no `Appendix-A` sheet, or no item rows can be read, the file fails with a clear error, is logged as `Failure`, and a copy is placed in a `quarantine` folder next to it. The other files continue. If a file parses but does not have 17 cities and 51 items, a warning is written to the log `message`.
- A new column: `spark.databricks.delta.schema.autoMerge.enabled` is set, so a `MERGE` can add a column that the source gained. Silver selects its columns explicitly, so an extra Bronze column never breaks it.
- A type change (for example a number arriving as text): `try_cast` yields NULL, and the row is quarantined with a reason, without stopping the batch.

### Audit logging

`write_log()` writes a row to `pipeline_execution_logs` for every unit of work, on success and on failure. Raw-to-Bronze logs one row per file. Bronze-to-Silver logs one row per run, with the date range or source file as the parameter. Each row has the layer, the parameter, start and end times, status, rows inserted, rows updated, rows quarantined and a message.

## 5. Execution guide

### 5.1 One-time setup

1. Create the volume folders: `/Volumes/workspace/spi/raw/xlsx`, `/Volumes/workspace/spi/raw/pdf` and `/Volumes/workspace/spi/raw/manifest`.
2. On a PC, run the scraper to build the link list:

   ```
   pip install playwright
   playwright install chromium
   python pbs_spi_scraper.py
   ```

   This writes `spi_weeks_flags.csv`.
3. Upload `spi_weeks_flags.csv` to `/Volumes/workspace/spi/raw/manifest/`.

Folder defaults live only in `00_config.py` (`RAW_FOLDER`, `RAW_PDF_FOLDER`).

### 5.2 Run order

1. `01_download_annexures`: downloads the files listed in the CSV into the `xlsx` and `pdf` folders. Files that already exist are skipped.
2. `01_raw_to_bronze`: parses the files into Bronze.
3. `02_bronze_to_silver`: builds Silver.

### 5.3 Widgets

`01_raw_to_bronze`

| Widget | Meaning |
|---|---|
| `run_mode` | `incremental` skips files that already loaded successfully. `full` processes every file in range again (safe, because of MERGE). |
| `file_types` | `both`, `xlsx` or `pdf`. |
| `raw_folder` | Folder with the xlsx files. Empty skips Excel. |
| `raw_pdf_folder` | Folder with the pdf files. Empty skips PDF. |
| `start_date`, `end_date` | Optional `YYYY-MM-DD`. Only files whose name carries a date in this range are used. |
| `batch_id` | Optional run label. Empty generates a new one. |

`02_bronze_to_silver`

| Widget | Meaning |
|---|---|
| `run_mode` | `incremental` or `full`, recorded in the log. Both are safe to repeat because of MERGE. |
| `start_date`, `end_date` | Optional `YYYY-MM-DD`. Only Bronze rows with a price date in this range are processed. |
| `source_file` | Optional. Process only this Bronze file. |
| `batch_id` | Optional run label. |

### 5.4 Standard incremental load

Run `01_raw_to_bronze` with `run_mode = incremental` and every other widget empty. Only files that have not loaded successfully before are processed. Then run `02_bronze_to_silver` with all widgets empty.

### 5.5 Backfills

| Goal | `01_raw_to_bronze` | `02_bronze_to_silver` |
|---|---|---|
| Reload everything | `run_mode = full`, other widgets empty | `run_mode = full`, other widgets empty |
| Reload a period | `run_mode = full`, `start_date = 2025-01-01`, `end_date = 2025-03-31` | `run_mode = full`, same `start_date` and `end_date` |
| Reload one week | `run_mode = full`, `start_date` and `end_date` both set to that week's date | `source_file = Annex_24.09.2026.xlsx`, or `start_date` and `end_date` set to that date |
| Only PDF or only Excel | `file_types = pdf` or `xlsx` | not needed |
| Files in another folder | set `raw_folder` and/or `raw_pdf_folder` | not needed |
| Tag a run | set `batch_id` to any label | set `batch_id` to any label |

Backfills never create duplicates, and unchanged rows are not rewritten. The same widgets can be passed as job parameters when the notebooks are scheduled in a Databricks Job.

## 6. Verifying the pipeline

Idempotency: run `02_bronze_to_silver` twice on the same data. The second run should print `inserted=0 updated=0`. Then this query should return no rows:

```sql
SELECT price_date, city_code, item_no, COUNT(*) AS copies
FROM workspace.spi.silver_spi_prices
GROUP BY price_date, city_code, item_no
HAVING COUNT(*) > 1;
```

Completeness: every week should show 867 rows, 17 cities and 51 items.

```sql
SELECT price_date, COUNT(*) AS row_count, COUNT(DISTINCT city_code) AS cities,
       COUNT(DISTINCT item_no) AS items,
       SUM(CASE WHEN is_available THEN 0 ELSE 1 END) AS unavailable
FROM workspace.spi.silver_spi_prices
GROUP BY price_date
ORDER BY price_date;
```

One name and one unit per item (should return no rows):

```sql
SELECT item_no, COUNT(DISTINCT item_name) AS names, COUNT(DISTINCT unit) AS units
FROM workspace.spi.silver_spi_prices
GROUP BY item_no
HAVING COUNT(DISTINCT item_name) > 1 OR COUNT(DISTINCT unit) > 1;
```

Rejected rows and why:

```sql
SELECT source_file, reject_reason, COUNT(*) AS rows_rejected
FROM workspace.spi.silver_spi_quarantine
GROUP BY source_file, reject_reason
ORDER BY source_file;
```

Audit trail:

```sql
SELECT layer, parameter, run_mode, status, rows_inserted, rows_updated, rows_quarantined,
       start_time, end_time, message
FROM workspace.spi.pipeline_execution_logs
ORDER BY start_time DESC;
```

Exported notebook outputs showing these runs are in `proof/`.

## 7. Known limitations

- Before the first full load of Silver, the PDF parser should be checked on a few files by comparing the parsed rows against the PDF. PDF extraction depends on the page layout, so a layout change shows up as a row-count warning in the log (not 867 rows, 17 cities, 51 items).
- Two weeks have no usable annexure link on the website and are skipped by the download notebook: 2024-07-04 (only summary files are linked) and 2025-08-21 (the link points to the 13-08-2025 file).
- The 2025-06-04 annexure link is a generic `Annex.pdf` with no date in its name, so its week cannot be checked from the file name.
- Delta does not enforce primary keys. Uniqueness comes from the MERGE keys.
- Only the weekly Appendix-A price table is loaded. The summary tables in the annexure (fertilizer, cement, CNG, wages, wheat) are not.
