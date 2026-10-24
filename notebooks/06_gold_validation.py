# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06_gold_validation
# MAGIC Run after `05_silver_to_gold`. It checks the Gold tables, shows the optimization evidence for the report,
# MAGIC previews the answers to the three business questions, and creates the audit views for Power BI.
# MAGIC Nothing here changes the Gold tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table sizes and clustering (for the report)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.spi.fact_spi_price;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the OPTIMIZE runs and how many files they compacted
# MAGIC SELECT version, timestamp, operation, operationMetrics
# MAGIC FROM (DESCRIBE HISTORY workspace.spi.fact_spi_price)
# MAGIC WHERE operation IN ('OPTIMIZE', 'MERGE', 'CREATE TABLE')
# MAGIC ORDER BY version;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query plan of a typical dashboard query (for the report)
# MAGIC Look for the filter on `date_key` being pushed into the scan of the fact table, the broadcast of the small dimensions, and the shuffle for the GROUP BY.

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT d.year_month, i.category, AVG(f.avg_price) AS avg_price, AVG(f.wow_pct) AS avg_wow_pct
# MAGIC FROM workspace.spi.fact_spi_price f
# MAGIC JOIN workspace.spi.dim_date d ON f.date_key = d.date_key
# MAGIC JOIN workspace.spi.dim_item i ON f.item_key = i.item_key
# MAGIC WHERE f.date_key >= 20260101
# MAGIC GROUP BY d.year_month, i.category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- every week should have 867 fact rows (17 cities x 51 items)
# MAGIC SELECT COUNT(*) AS weeks,
# MAGIC        SUM(CASE WHEN n = 867 THEN 1 ELSE 0 END) AS complete_weeks,
# MAGIC        MIN(n) AS fewest_rows, MAX(n) AS most_rows
# MAGIC FROM (SELECT date_key, COUNT(*) AS n FROM workspace.spi.fact_spi_price GROUP BY date_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- how much of the fact table has week-on-week and year-on-year values (the rest is NULL: no week to compare with)
# MAGIC SELECT COUNT(*) AS rows_total,
# MAGIC        COUNT(wow_pct) AS with_wow, ROUND(100 * COUNT(wow_pct) / COUNT(*), 1) AS wow_pct_coverage,
# MAGIC        COUNT(yoy_pct) AS with_yoy, ROUND(100 * COUNT(yoy_pct) / COUNT(*), 1) AS yoy_pct_coverage
# MAGIC FROM workspace.spi.fact_spi_price
# MAGIC WHERE is_available;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- food / non-food tags: check every item is in the right group
# MAGIC SELECT item_key, item_name, unit, category FROM workspace.spi.dim_item ORDER BY item_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview of the three business answers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: which city has the sharpest increase over the last 4 weeks (latest week)
# MAGIC SELECT c.city_name, g.price_index, g.pct_4w, g.wow_pct
# MAGIC FROM workspace.spi.gold_city_weekly_index g
# MAGIC JOIN workspace.spi.dim_city c ON g.city_key = c.city_key
# MAGIC WHERE g.date_key = (SELECT MAX(date_key) FROM workspace.spi.gold_city_weekly_index)
# MAGIC ORDER BY g.pct_4w DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: which items moved most this week, with their food / non-food group
# MAGIC SELECT i.item_name, i.category, v.national_avg_price, v.wow_pct, v.rolling_std_12w, v.max_swing_12w
# MAGIC FROM workspace.spi.gold_item_volatility v
# MAGIC JOIN workspace.spi.dim_item i ON v.item_key = i.item_key
# MAGIC WHERE v.date_key = (SELECT MAX(date_key) FROM workspace.spi.gold_item_volatility)
# MAGIC ORDER BY ABS(v.wow_pct) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3: how does the latest week compare with history (national index)
# MAGIC SELECT d.week_ending_date, n.price_index, n.wow_pct, n.yoy_pct
# MAGIC FROM workspace.spi.gold_national_weekly_index n
# MAGIC JOIN workspace.spi.dim_date d ON n.date_key = d.date_key
# MAGIC ORDER BY d.week_ending_date DESC
# MAGIC LIMIT 12;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit views for the Power BI pipeline-health page

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.spi.vw_pipeline_runs AS
# MAGIC SELECT log_id, batch_id, layer, run_mode, parameter, start_time, end_time, status,
# MAGIC        rows_inserted, rows_updated, rows_quarantined, message,
# MAGIC        CAST(start_time AS DATE) AS run_date,
# MAGIC        unix_timestamp(end_time) - unix_timestamp(start_time) AS duration_seconds
# MAGIC FROM workspace.spi.pipeline_execution_logs;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.spi.vw_quarantine_summary AS
# MAGIC SELECT source_file, reject_reason, COUNT(*) AS rows_rejected
# MAGIC FROM workspace.spi.silver_spi_quarantine
# MAGIC GROUP BY source_file, reject_reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT layer, status, COUNT(*) AS runs, SUM(rows_inserted) AS inserted, SUM(rows_updated) AS updated,
# MAGIC        SUM(rows_quarantined) AS quarantined, ROUND(AVG(duration_seconds), 1) AS avg_seconds
# MAGIC FROM workspace.spi.vw_pipeline_runs
# MAGIC GROUP BY layer, status
# MAGIC ORDER BY layer, status;