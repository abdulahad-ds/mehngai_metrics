# Weekly SPI Prices Lakehouse (Bronze / Silver on Databricks)

A PySpark pipeline that loads the weekly **Sensitive Price Indicator (SPI) Annexure, Appendix-A "Consumer Prices of Essential Items"** into a Delta Lakehouse on Databricks. Each weekly file lists **51 items** across **17 cities**, with a minimum, average and maximum price per item and city. The files arrive in two formats, Excel (`.xlsx`) and PDF, kept in separate folders. Both formats go through the same pipeline.

```
 raw .xlsx folder ─┐                                                    ┌─> silver_spi_quarantine (bad records)
                   ├─> 01_raw_to_bronze ─> bronze_spi_prices ─> 02_bronze_to_silver ─> silver_spi_prices
 raw .pdf  folder ─┘          │                                          │
                              └────────── pipeline_execution_logs ───────┘
```

## Repository layout

```
notebooks/
  00_config.py              shared settings, explicit schemas, Excel + PDF parsers, logging function
  01_raw_to_bronze.py       Raw -> Bronze (parameterised, MERGE)
  02_bronze_to_silver.py    Bronze -> Silver (parameterised, MERGE, quarantine)
README.md
```

All notebooks are committed to this repository; `00_config` is loaded by the other two with `%run ./00_config`.

## Data models

Catalog / schema: `workspace.spi`. Every table has a `load_timestamp` column set when the row is written.

### Bronze: `workspace.spi.bronze_spi_prices`

Raw values exactly as parsed from the file; everything the parser reads stays as **text**. One row per file, item and city.

| Column | Type | Null | Description |
|---|---|---|---|
| `batch_id` | string | no | Run label of the load that wrote the row (widget `batch_id`) |
| `source_file` | string | no | File the row came from (lineage) **(PK)** |
| `price_date_text` | string | yes | `PRICES ON dd-mm-yyyy` text from the file |
| `item_sl_no` | string | no | Serial number of the item (1-51) **(PK)** |
| `item_description` | string | yes | Item name |
| `unit` | string | yes | Unit of the price (`1 Kg`, `Each`, `Per Litre`, ...) |
| `city_raw` | string | no | City with its code, e.g. `Islamabad (01)` **(PK)** |
| `min_price_raw` | string | yes | Minimum price, as text |
| `avg_price_raw` | string | yes | Average price, as text |
| `max_price_raw` | string | yes | Maximum price, as text |
| `load_timestamp` | timestamp | no | When the row was loaded |

**Primary key:** (`source_file`, `item_sl_no`, `city_raw`)

### Silver: `workspace.spi.silver_spi_prices`

Typed, validated and de-duplicated. One row per price date, city and item, no matter which file format it came from.

| Column | Type | Null | Description |
|---|---|---|---|
| `price_date` | date | no | Date of the prices **(PK)** |
| `city_code` | int | no | City code from `city_raw`, e.g. 1 **(PK)** |
| `city_name` | string | yes | City name, e.g. `Islamabad` |
| `item_no` | int | no | Item serial number **(PK)** |
| `item_name` | string | yes | Item name |
| `unit` | string | yes | Unit of the price |
| `min_price` | double | yes | Minimum price |
| `avg_price` | double | yes | Average price |
| `max_price` | double | yes | Maximum price |
| `is_available` | boolean | no | Whether a usable price exists for that item and city |
| `source_file` | string | yes | File the record came from |
| `batch_id` | string | yes | Batch that last wrote the row |
| `load_timestamp` | timestamp | no | When the row was loaded |

**Primary key:** (`price_date`, `city_code`, `item_no`) <!-- CHECK: must match the MERGE ON clause in 02_bronze_to_silver -->

Casting done on the way into Silver: `price_date_text` (dd-MM-yyyy) to `date`; `city_raw` split into `city_code` (int) and `city_name`; `item_sl_no` to `int`; the three price columns to `double`. <!-- CHECK: rule used for is_available -->

### Quarantine: `workspace.spi.silver_spi_quarantine`

Bronze records that fail validation are written here with the reason, and the rest of the batch continues.

| Column | Type | Description |
|---|---|---|
| `batch_id` | string | Batch that produced the record |
| `source_file` | string | File the record came from |
| `price_date_text`, `item_sl_no`, `item_description`, `unit`, `city_raw` | string | Bronze values, unchanged |
| `min_price_raw`, `avg_price_raw`, `max_price_raw` | string | Bronze values, unchanged |
| `reject_reason` | string | Why the record was rejected |
| `load_timestamp` | timestamp | When it was quarantined |

### Execution log: `workspace.spi.pipeline_execution_logs`

One row per processed file (Raw-to-Bronze) or per Silver run (Bronze-to-Silver), for both full and incremental loads.

| Column | Type | Description |
|---|---|---|
| `log_id` | string | Unique id of the log row **(PK)** |
| `batch_id` | string | Batch of the run; joins to `batch_id` in Bronze and Silver |
| `layer` | string | `Raw-to-Bronze` or `Bronze-to-Silver` |
| `run_mode` | string | `full` or `incremental` |
| `parameter` | string | File name processed, or the date range for Silver |
| `start_time`, `end_time` | timestamp | Start and end of the run |
| `status` | string | `Success` or `Failure` |
| `rows_inserted`, `rows_updated`, `rows_quarantined` | long | Row counts from MERGE and quarantine |
| `message` | string | Warning or error text |
| `load_timestamp` | timestamp | When the log row was written |

## How it works

**Explicit schemas.** All schemas are `StructType`/`StructField` objects defined in `00_config`. No schema inference is used. The parsers return plain text values and the DataFrame is created with `BRONZE_PARSE_SCHEMA`.

**Two file formats, one entry point.** `parse_file(path)` in `00_config` picks the parser by file extension.
- Excel: reads the `Appendix-A` sheet and finds the city header row.
- PDF: reads the Appendix-A pages with `pdfplumber` (three pages: cities 1-7, 8-14, 15-17). The summary tables after Appendix-A (fertilizer, cement, CNG, wages, wheat) are ignored.
- The price date is read from the file; if it is missing, the date in the file name is used.

**Raw to Bronze (`01_raw_to_bronze`).**
- Files are processed one at a time, so one bad file never stops the others.
- Rows are written with `MERGE INTO` on (`source_file`, `item_sl_no`, `city_raw`). A matching row is updated only if a value actually changed; otherwise it is left alone.
- A file that fails to parse is logged as `Failure` and copied to a `quarantine/` folder next to the raw files.
- A file with an unexpected number of cities or items is still loaded, and a warning is stored in the log `message`.

**Bronze to Silver (`02_bronze_to_silver`).** Casts and validates the Bronze rows, sends bad records to the quarantine table, and merges the good ones into Silver on (`price_date`, `city_code`, `item_no`). <!-- CHECK: describe exactly what the notebook does -->

**Idempotency.** Both layers write with `MERGE INTO`, so running the same input again gives the same tables with no duplicate rows (see "Proof checks" below).

**Schema drift.**
- Delta schema auto-merge is switched on in `00_config`, so a new source column is added to the target table by MERGE instead of failing it.
- Bronze keeps every value as text, so a changed data type in the source does not break the load.
- Rows with values that cannot be cast, or that break a rule, go to `silver_spi_quarantine` with a `reject_reason`.
- A file whose layout can no longer be parsed is logged as `Failure` and copied to `quarantine/`; the other files still load.

## Setup

1. Use a Databricks workspace (Free Edition works). Clone this repository into the workspace (Workspace > Create > Git folder).
2. Create two volumes folders for the raw files, for example:
   - `/Volumes/workspace/spi/raw` for the `.xlsx` files
   - `/Volumes/workspace/spi/raw_pdf` for the `.pdf` files
3. If your folders are different, change `RAW_FOLDER` and `RAW_PDF_FOLDER` at the top of `00_config`. (They can also be overridden by widgets at run time.)
4. `openpyxl` and `pdfplumber` are needed. `01_raw_to_bronze` lists them in its dependency header, and `00_config` installs `pdfplumber` itself if it is missing.
5. The tables are created automatically on the first run (`ensure_tables()`).

## Running the pipeline

### Parameters

`01_raw_to_bronze` (widgets at the top of the notebook):

| Widget | Values | Meaning |
|---|---|---|
| `run_mode` | `incremental` (default), `full` | `incremental` skips files that already have a `Success` row for `Raw-to-Bronze` in the log table. `full` processes every file in range again (safe, because of MERGE). |
| `file_types` | `both` (default), `xlsx`, `pdf` | Which file types to pick up |
| `raw_folder` | folder path | Folder with the `.xlsx` files (empty = skip Excel) |
| `raw_pdf_folder` | folder path | Folder with the `.pdf` files (empty = skip PDF) |
| `start_date`, `end_date` | `YYYY-MM-DD` or empty | Only files whose name contains a date in this range. Empty = no limit. Files with no date in the name are always included. |
| `batch_id` | text or empty | Label of the run. Empty = generated automatically. |

`02_bronze_to_silver` takes the same kind of parameters. <!-- CHECK: list the real widget names of 02_bronze_to_silver -->

### Normal incremental load (new weekly files)

1. Put the new `.xlsx` and `.pdf` files into their folders.
2. Run `01_raw_to_bronze` with the defaults (`run_mode = incremental`, no dates). Only files not yet logged as successful are loaded.
3. Run `02_bronze_to_silver` with `run_mode = incremental`.

This can be scheduled as a Databricks Job (for example weekly) that runs the two notebooks in order.

### Backfill an older period

Run with `run_mode = full` and a date range. Nothing in the code needs editing.

```python
# Re-load all files dated in 2025 into Bronze, then rebuild Silver for the same period
dbutils.notebook.run("01_raw_to_bronze", 3600, {
    "run_mode": "full", "start_date": "2025-01-01", "end_date": "2025-12-31",
    "batch_id": "backfill_2025"})
dbutils.notebook.run("02_bronze_to_silver", 3600, {
    "run_mode": "full", "start_date": "2025-01-01", "end_date": "2025-12-31",
    "batch_id": "backfill_2025"})   # CHECK widget names
```

Other examples:
- Reload a single file: set `start_date` and `end_date` to that file's date.
- Excel files only: `file_types = xlsx`.
- A different folder: set `raw_folder` / `raw_pdf_folder`.
- A file that was corrected and re-uploaded under the **same name** is skipped in incremental mode; reload it with `run_mode = full`.

## Proof checks (SQL)

**No duplicates after a re-run.** Run the same load twice and compare. Re-running should insert 0 rows and the counts should not change.

```sql
SELECT COUNT(*) AS bronze_rows FROM workspace.spi.bronze_spi_prices;
SELECT COUNT(*) AS silver_rows FROM workspace.spi.silver_spi_prices;

-- must return no rows
SELECT source_file, item_sl_no, city_raw, COUNT(*) AS n
FROM workspace.spi.bronze_spi_prices
GROUP BY source_file, item_sl_no, city_raw HAVING COUNT(*) > 1;

SELECT price_date, city_code, item_no, COUNT(*) AS n
FROM workspace.spi.silver_spi_prices
GROUP BY price_date, city_code, item_no HAVING COUNT(*) > 1;
```

**Row count per file.** Each file should show 867 rows (51 items x 17 cities) and 17 cities.

```sql
SELECT source_file, price_date_text, COUNT(*) AS row_count, COUNT(DISTINCT city_raw) AS cities
FROM workspace.spi.bronze_spi_prices
GROUP BY source_file, price_date_text ORDER BY source_file;
```

**Execution log.**

```sql
SELECT layer, run_mode, parameter, start_time, end_time, status,
       rows_inserted, rows_updated, rows_quarantined, message
FROM workspace.spi.pipeline_execution_logs
ORDER BY start_time DESC;
```

**Quarantined records.**

```sql
SELECT reject_reason, COUNT(*) AS n
FROM workspace.spi.silver_spi_quarantine
GROUP BY reject_reason;
```
