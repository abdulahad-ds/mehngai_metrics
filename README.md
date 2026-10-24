# Mehngai Metrics


Every week the Pakistan Bureau of Statistics (PBS) publishes a price survey covering 51 everyday items in 17 cities. Most coverage only quotes the national figure. We wanted the city and item detail, so we built a pipeline on Databricks that loads every weekly release since July 2023, cleans it up, and feeds a Power BI dashboard that shows where prices are climbing and which items are behind it.


## What the dashboard answers

Three questions drove the design. Which city has seen the sharpest rise in the cost of living this month? Which items are pushing it? And how does this week compare with the weeks before it? The first is a city heatmap, the second a ranking of items by weekly change split into food and non-food, and the third a trend of week-on-week and year-on-year change with a line at zero so deflation weeks stand out. A fourth page shows the health of the pipeline itself, built from its own audit log.

## The data

The source is the Weekly Sensitive Price Indicator page at https://www.pbs.gov.pk/price-statistics/. Each week PBS publishes an annexure file with the minimum, average and maximum price for every item in every city. We load that annexure and nothing else, because it is the only release that has a city breakdown.

What we ended up with:

- 141 weekly releases, from 13 July 2023 to 22 October 2026
- 867 rows per week (17 cities times 51 items), 122,247 rows in total
- 143 source files: 86 PDFs and 57 Excel files. Recent weeks come as Excel, older weeks exist only as PDF, so we had to parse both.
- 499 rows where an item simply was not sold in a city that week

The series has holes. There are 14 places where more than eight days pass between two weeks, the longest being nine weeks between 12 September and 7 November 2024. Two of those weeks exist on the site but have no usable annexure: 4 July 2024 only links to summary files, and the link for 21 August 2025 points at the file for 13 August. We skip both rather than load the wrong data.

## How it fits together

The first obstacle was getting the list of files. The table of releases on the PBS site is filled in by JavaScript, so the page HTML does not contain it. Reading it needs a real browser, and the browser we use (Playwright with Chromium) cannot be downloaded on Databricks Free Edition. So the history is collected once, on a laptop, by `pbs_spi_scraper.py`, which writes a CSV of links. We upload that CSV to Databricks and `01_download_full` downloads the files from it.

Weekly runs do not need the browser. The newest week's links are in the plain page HTML, so `02_download_latest` reads them from there. It also tries the usual file name for the last four Thursdays, which means a week the job missed gets picked up on the next run.

From there the data moves through three layers:

**Bronze** holds exactly what we read from the files, as text, one row per file, item and city. We parse the Excel files with pandas and the PDFs with pdfplumber, and both end up in the same table. Nothing is converted here, so a strange value in the source can never crash the load.

**Silver** is where the data becomes usable. Dates become dates, prices become numbers, the city code is split out of text like "Islamabad (01)", and duplicates are resolved. Rows that cannot be trusted go to a quarantine table with a reason instead of stopping the run.

**Gold** is a small star schema for Power BI: three dimensions (date, city, item), a price fact table, and three summary tables for the dashboard (a price index per city, a national index, and item volatility).

Every notebook that transforms data writes to `pipeline_execution_logs`, and the dashboard's last page reads from it.

A Databricks Job runs the weekly chain on Saturdays: download the latest file, load Bronze, build Silver, build Gold. It has been running on schedule, so a new release reaches the Gold tables without anyone touching it. Only the Power BI refresh is still manual.

## What is in the repo

The notebooks are numbered in the order they run.

| Notebook | What it does |
|---|---|
| `pbs_spi_scraper.py` | Runs on a PC. Reads the SPI table with a browser and writes `spi_weeks_flags.csv`. |
| `00_config` | Shared setup: table names, explicit schemas, the Excel and PDF parsers, the log function. Every other notebook loads it. |
| `01_download_full` | Downloads the files listed in the CSV. Used for the first load and for backfills. |
| `02_download_latest` | The weekly download. Finds the newest annexure without a browser. |
| `03_raw_to_bronze` | Parses the files and merges them into Bronze. |
| `04_bronze_to_silver` | Casts, validates, deduplicates and merges into Silver. |
| `05_silver_to_gold` | Builds the dimensions, the fact table and the summary tables. |
| `06_gold_validation` | Checks the Gold tables, prints a query plan, previews the three answers, and creates the audit views for Power BI. |

The Power BI file and the project report are in the repo too.

## Data models

Everything lives in `workspace.spi`. Delta does not enforce primary keys, so the keys below are enforced by the MERGE statements that load each table.

### Bronze: `bronze_spi_prices`

Primary key: `source_file`, `item_sl_no`, `city_raw`. All values are text.

| Column | Type | Notes |
|---|---|---|
| `batch_id` | string | Run that loaded the row |
| `source_file` | string | For example `Annex_24.09.2026.xlsx` |
| `price_date_text` | string | The date as written, dd-MM-yyyy |
| `item_sl_no` | string | Item number, 1 to 51 |
| `item_description` | string | Item name as written in that file |
| `unit` | string | Unit as written in that file |
| `city_raw` | string | City with its code, like `Lahore (05)` |
| `min_price_raw`, `avg_price_raw`, `max_price_raw` | string | Prices as text |
| `load_timestamp` | timestamp | When the row was ingested |

### Silver: `silver_spi_prices`

Primary key: `price_date`, `city_code`, `item_no`.

| Column | Type | Notes |
|---|---|---|
| `price_date` | date | |
| `city_code` | int | |
| `city_name` | string | |
| `item_no` | int | |
| `item_name` | string | One standard name per item |
| `unit` | string | One standard unit per item |
| `min_price`, `avg_price`, `max_price` | double | NULL when the item was not available |
| `is_available` | boolean | False when there was no price |
| `source_file` | string | Where the row came from |
| `batch_id` | string | Silver run that wrote it |
| `load_timestamp` | timestamp | When it was processed |

### Quarantine: `silver_spi_quarantine`

Same text columns as Bronze, plus `batch_id`, `reject_reason` and `load_timestamp`. A row lands here for a bad date, city code or item number, a price that is not a number, a negative price, or a minimum, average and maximum that are out of order. At the time of writing it is empty, because none of the real data has needed it yet.

### Audit log: `pipeline_execution_logs`

Primary key: `log_id`. One row per unit of work, with `batch_id`, `layer` (Raw-to-Bronze, Bronze-to-Silver or Silver-to-Gold), `run_mode`, `parameter`, `start_time`, `end_time`, `status`, `rows_inserted`, `rows_updated`, `rows_quarantined`, `message` and `load_timestamp`. Bronze logs a row per file, Silver one per run, Gold one per table.

### Gold

Every table joins to `dim_date` on `date_key`. All of them also carry a `load_timestamp`.

| Table | Key | Rows | Columns |
|---|---|---|---|
| `dim_date` | `date_key` (yyyymmdd) | 141 | `week_ending_date`, `cal_year`, `cal_quarter`, `cal_month`, `month_name`, `iso_week`, `year_month` |
| `dim_city` | `city_key` | 17 | `city_name` |
| `dim_item` | `item_key` | 51 | `item_name`, `unit`, `category` (Food or Non-food) |
| `fact_spi_price` | `date_key`, `city_key`, `item_key` | 122,247 | `min_price`, `avg_price`, `max_price`, `is_available`, `wow_pct`, `yoy_pct` |
| `gold_city_weekly_index` | `date_key`, `city_key` | 2,397 | `price_index`, `items_in_index`, `wow_pct`, `pct_4w`, `yoy_pct` |
| `gold_national_weekly_index` | `date_key` | 141 | `price_index`, `wow_pct`, `yoy_pct` |
| `gold_item_volatility` | `date_key`, `item_key` | 7,191 | `national_avg_price`, `wow_pct`, `rolling_std_12w`, `max_swing_12w`, `weeks_in_window` |

In the star, `fact_spi_price` is the centre, with `dim_date`, `dim_city` and `dim_item` around it. The three summary tables hang off the same dimensions: the city index off `dim_date` and `dim_city`, the item volatility off `dim_date` and `dim_item`, and the national index off `dim_date`.

A few definitions, because the numbers are easy to misread:

- `wow_pct`, `pct_4w` and `yoy_pct` are percent changes against the same city and item 7, 28 or 364 days earlier. We take the closest date within three days, and the value is NULL if there is none. The first year-on-year value is 11 July 2024, exactly 364 days after the first week.
- `price_index` is our own simple index. For each city, every item's average price is divided by its price in the first week (13 July 2023), multiplied by 100 and averaged, with equal weights. It is not the official weighted PBS index, which uses weights we do not have.
- `rolling_std_12w` and `max_swing_12w` describe how much an item's national weekly change has moved over its last 12 weeks of data.
- `category` comes from a keyword rule on the item name, since the source has no category. Currently 32 items are Food and 19 are Non-food, and any wrong one can be fixed in the `OVERRIDES` list in `05_silver_to_gold`.

## Running it

### First time

1. On a PC, install Playwright and run the scraper:

   ```
   pip install playwright
   playwright install chromium
   python pbs_spi_scraper.py
   ```

2. Upload the resulting `spi_weeks_flags.csv` to `/Volumes/workspace/spi/raw/manifest/`.
3. In Databricks, run `01_download_full`, then `03_raw_to_bronze`, `04_bronze_to_silver` and `05_silver_to_gold`.

### Every week

The Job does it. For a manual run, use `02_download_latest`, `03_raw_to_bronze`, `04_bronze_to_silver` and `05_silver_to_gold` with `run_mode = incremental` and every other widget empty. If the Job missed a few weeks, run `02_download_latest` once with a larger `weeks_back`.

### Backfills and reloads

Nothing in the notebooks is hardcoded to a date or a path. Everything is a widget, and the same names work as Job parameters.

| You want to | `03_raw_to_bronze` | `04_bronze_to_silver` | `05_silver_to_gold` |
|---|---|---|---|
| Reload everything | `run_mode = full` | `run_mode = full` | `run_mode = full` |
| Reload a period | `run_mode = full`, plus `start_date` and `end_date` | `start_date` and `end_date` | `start_date` and `end_date` |
| Reload a single week or file | `start_date` and `end_date` set to that week | `source_file` set to the file name | `start_date` and `end_date` set to that week |
| Only Excel or only PDF | `file_types` | not needed | not needed |
| Use another folder | `raw_folder`, `raw_pdf_folder` | not needed | not needed |
| Label the run | `batch_id` | `batch_id` | `batch_id` |

Dates are `YYYY-MM-DD`. In Bronze they are matched against the date in the file name, in Silver against the price date, and in Gold against the weeks written.

`run_mode` matters in Bronze, where `incremental` skips files that already loaded successfully and `full` goes through them again. In Silver and Gold both modes are safe to repeat and the mode is only recorded in the log. Gold always recalculates from all of Silver, because week-on-week and rolling values need the earlier weeks, and a date range only limits what gets written. If you ever add a week earlier than the current first week, run Gold without dates, since the index base changes.

## Why a second run changes nothing

Every table is loaded with `MERGE INTO`, keyed on the primary keys above. A row that already exists is updated only if one of its values actually changed. So running a notebook twice on the same data inserts nothing and updates nothing, and a backfill can be repeated as often as needed without creating duplicates. Before each MERGE the source is reduced to one row per key, because Delta refuses to merge when several source rows match one target row. If two files describe the same week, the Excel one wins over the PDF one.

## What the data threw at us

Most of the work was not Spark, it was the source being inconsistent. A few examples of what we met and what we did about it.

**The same item with different names.** In 86 PDF files item 1 reads "Wheat Flour" with the unit "Bag 20 Kg"; in the 57 Excel files it is "Wheat Flour Bag" with "20 Kg". Item 41 has a footnote star in 142 files and none in one. Item 42 is "Gas Charges for Q1" in 123 files and "Gas Charges upto 3.3719 MMBTU" in 20 others. Left alone, one item would have split into several series. Silver now keeps one name and unit per item, taken from the newest week, and writes a warning to the log listing the variants so nobody has to wonder why a name changed. The item 42 wording may mean a different tariff slab, so be careful with its change around that point.

**Missing prices are zeros.** We expected markers like "NA", and we handle those too, but in practice PBS writes 0.00 for an item that is not sold in a city. A zero would drag every average down, so a row with zeros in all three prices is kept, with NULL prices and `is_available = false`. We do not drop it, because that would break the clean 867-row grid, and we do not fill it in.

**A messy website.** On some weeks the Excel link sits in the PDF column. On others the annexure and the summary links are swapped. File names come in many styles (`Annex_24.09.2026`, `Annex-09.10.2025-1`, `SPI-AnnexUSCP_30012025`), and one URL has a doubled slash. So the scraper classifies links by extension and by whether the file name contains "annex", not by column. `parse_file` also checks that the date inside a file matches its name, so a file filed under the wrong week cannot overwrite a good one.

**A boring failure that proved the point.** The first Excel load failed on one file because `openpyxl` was not installed. That failure was logged, the other files carried on, and once the dependency was added the file loaded normally. It is the one failure in the log, and it is a good example of why each file is processed on its own.

## Power BI

Open the `.pbix` file and refresh. To connect from scratch, use the Databricks connector in Power BI Desktop with the server hostname and HTTP path of a SQL warehouse (both are on the warehouse's Connection details tab) and a personal access token. Choose Import mode. The Gold tables are only a few megabytes, so Import is fast and the warehouse is only used when you refresh.

The model is a normal star: one-to-many relationships from `dim_date`, `dim_city` and `dim_item` to the fact and summary tables, filtering in one direction. `dim_date` is not marked as a date table, because Power BI requires every calendar day and ours has one row per week. That does not matter here since the changes are already calculated in Gold.

The percent columns are in percent units (2.5 means 2.5%), so the measures show them as plain numbers with a % sign.

The four pages are Overview, Cities, Items and Pipeline health. The last one uses two views created by `06_gold_validation`, `vw_pipeline_runs` and `vw_quarantine_summary`.

## Checking that it works

After a full load, these should all come out clean. The first query should return no rows:

```sql
SELECT price_date, city_code, item_no, COUNT(*) AS copies
FROM workspace.spi.silver_spi_prices
GROUP BY price_date, city_code, item_no
HAVING COUNT(*) > 1;
```

Every week should have 867 rows in the fact table:

```sql
SELECT d.week_ending_date, COUNT(*) AS fact_rows
FROM workspace.spi.fact_spi_price f
JOIN workspace.spi.dim_date d ON f.date_key = d.date_key
GROUP BY d.week_ending_date
ORDER BY d.week_ending_date;
```

Every item should have one name and one unit:

```sql
SELECT item_no, COUNT(DISTINCT item_name) AS names, COUNT(DISTINCT unit) AS units
FROM workspace.spi.silver_spi_prices
GROUP BY item_no
HAVING COUNT(DISTINCT item_name) > 1 OR COUNT(DISTINCT unit) > 1;
```

`05_silver_to_gold` also runs its own checks at the end (no repeated keys, no fact row pointing at a missing dimension row) and logs the result.

## Decisions we would defend

**No partitions.** We planned a partitioned Bronze path in the proposal and dropped it. The biggest table is about 1 MB, so a partition per week would hold 867 rows each and create a pile of tiny files. The fact table is clustered on `date_key` and `item_key` instead, which are the columns the dashboard filters on.

**No personal data.** The data is a government price survey, aggregated by city and item, with no names, contact details or anything about individual shops or enumerators. So there was nothing to mask.

**Databricks Free Edition.** The proposal named Community Edition, but we ran on Free Edition, which is serverless only. That means no cluster to size or forget to stop, and also no classic Spark UI. For execution plans we use `explain` and the query profile instead.

**Our own index.** The annexure does not include the weights PBS uses for its official index, so we do not pretend to reproduce it. Ours is an equal-weight basket against a fixed base week, good for comparing cities and weeks, not for quoting as the official inflation rate.

## Speed and cost

The data is tiny, so this is cheap. A full Gold build takes about a minute (66 seconds in our log), Silver about 17 seconds, and loading a single weekly file into Bronze about 6 seconds. Raw files take about 281 MB, nearly all of it PDFs, and the Delta tables together are about 3 MB. Free Edition has no bill, so the report works out an estimate from these run times instead.

One thing we noticed: the fact table's MERGE reads the Silver data about eight times, because the build uses the same source for the week-on-week and year-on-year steps and serverless compute does not allow caching. It is fast enough at this size, but it is the first thing we would fix if the data grew.

## What it does not do

The index is ours, not PBS's. Because of the gaps in the series, some week-on-week and year-on-year values are NULL. Rice IRRI-6/9 has no price in Gujranwala, Sialkot and Lahore, so those three cities have 50 items in their index and the others have 51. PDF parsing depends on the page layout, so if PBS redesigns the file the parser will need updating (the check for 17 cities and 51 items per file will flag it). Silver and Gold recalculate from all earlier data every run, which is fine today and would need an incremental version at larger scale. And the quarantine path is in place but has not yet been triggered by real data.

## Authors

M. Abdul Ahad and Laiba Shahzad, BDS-5A, FAST-NUCES Lahore.
