# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05_silver_to_gold
# MAGIC Builds the Gold star schema from Silver and refreshes it with MERGE (safe to re-run, no duplicates).
# MAGIC One log row per Gold table is written to `pipeline_execution_logs` with layer `Silver-to-Gold`.
# MAGIC
# MAGIC **Tables**
# MAGIC | Table | Grain | Key |
# MAGIC |---|---|---|
# MAGIC | `dim_date` | one row per week-ending date | `date_key` |
# MAGIC | `dim_city` | one row per city | `city_key` |
# MAGIC | `dim_item` | one row per item | `item_key` |
# MAGIC | `fact_spi_price` | week, city, item | `date_key`, `city_key`, `item_key` |
# MAGIC | `gold_city_weekly_index` | week, city | `date_key`, `city_key` |
# MAGIC | `gold_national_weekly_index` | week | `date_key` |
# MAGIC | `gold_item_volatility` | week, item | `date_key`, `item_key` |
# MAGIC
# MAGIC **Definitions**
# MAGIC - `wow_pct`, `pct_4w`, `yoy_pct`: percent change against the same row 7, 28 or 364 days earlier (closest date within 3 days). NULL if no such week exists.
# MAGIC - `price_index`: fixed basket, equal weights. For each city, every item's average price divided by its price in the first week of the data, times 100, averaged over the items. Base week = 100. This is not the official weighted PBS SPI.
# MAGIC - `gold_national_weekly_index`: average of the city indexes.
# MAGIC - `rolling_std_12w`, `max_swing_12w`: standard deviation and largest absolute value of the item's national weekly % change over its last 12 weeks of data.
# MAGIC
# MAGIC **Parameters**
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `run_mode` | `incremental` or `full`. Recorded in the log. Both are safe to repeat. |
# MAGIC | `start_date`, `end_date` | Optional `YYYY-MM-DD`. Only fact and aggregate rows with a week in this range are written. Empty = all. |
# MAGIC | `batch_id` | Optional run label. |
# MAGIC
# MAGIC Changes are always calculated from all of Silver (the week-on-week and rolling values need earlier weeks). MERGE only writes rows whose values changed.
# MAGIC
# MAGIC **Read the numbers with these limits in mind**
# MAGIC - The index base is the earliest week in Silver. If an earlier week is added later (a backfill), every index value changes, so run this notebook with `start_date` and `end_date` empty afterwards. A date-scoped run only rewrites the weeks in its range.
# MAGIC - An item with no price in a city (for example Rice IRRI-6/9 in Gujranwala, Sialkot and Lahore) is left out of that city's basket for good, so those cities have 50 items in the index and the others 51. Check `items_in_index`.
# MAGIC - If an item has no price in a week, the city index for that week averages fewer items and can move for that reason alone.
# MAGIC - `national_avg_price` averages the cities that have a price, so an item that appears or disappears in one city moves it. Item 42 (gas charges) changed its wording on 24-09-2026, which may mean a different tariff slab; look at its `wow_pct` for that week before using it.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("run_mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("batch_id", "")

run_mode = dbutils.widgets.get("run_mode")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or str(uuid.uuid4())
print("run_mode:", run_mode, "| range:", start_date or "-", "to", end_date or "-", "| batch:", batch_id)

G = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

# COMMAND ----------

# Gold tables with explicit column types. The fact table is clustered (Liquid Clustering) on the keys most used in filters.
TABLES = {
    "dim_date": ("date_key INT NOT NULL, week_ending_date DATE, cal_year INT, cal_quarter INT, cal_month INT, "
                 "month_name STRING, iso_week INT, year_month STRING, load_timestamp TIMESTAMP", ""),
    "dim_city": ("city_key INT NOT NULL, city_name STRING, load_timestamp TIMESTAMP", ""),
    "dim_item": ("item_key INT NOT NULL, item_name STRING, unit STRING, category STRING, load_timestamp TIMESTAMP", ""),
    "fact_spi_price": ("date_key INT NOT NULL, city_key INT NOT NULL, item_key INT NOT NULL, min_price DOUBLE, "
                       "avg_price DOUBLE, max_price DOUBLE, is_available BOOLEAN, wow_pct DOUBLE, yoy_pct DOUBLE, "
                       "load_timestamp TIMESTAMP", "CLUSTER BY (date_key, item_key)"),
    "gold_city_weekly_index": ("date_key INT NOT NULL, city_key INT NOT NULL, price_index DOUBLE, items_in_index INT, "
                               "wow_pct DOUBLE, pct_4w DOUBLE, yoy_pct DOUBLE, load_timestamp TIMESTAMP",
                               "CLUSTER BY (date_key)"),
    "gold_national_weekly_index": ("date_key INT NOT NULL, price_index DOUBLE, wow_pct DOUBLE, yoy_pct DOUBLE, "
                                   "load_timestamp TIMESTAMP", ""),
    "gold_item_volatility": ("date_key INT NOT NULL, item_key INT NOT NULL, national_avg_price DOUBLE, wow_pct DOUBLE, "
                             "rolling_std_12w DOUBLE, max_swing_12w DOUBLE, weeks_in_window INT, "
                             "load_timestamp TIMESTAMP", ""),
}
for name, (cols, cluster) in TABLES.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {G(name)} ({cols}) {cluster}")

# COMMAND ----------

# Helpers
def date_key():
    return F.date_format("price_date", "yyyyMMdd").cast("int")


def with_prior(df, keys, col, days, out, tol=3):
    """Adds `out` = % change of `col` against the same keys `days` days earlier (closest date within +-tol days)."""
    prior = (df.select(*keys, F.col("price_date").alias("p_date"), F.col(col).alias("p_val"))
               .withColumn("off", F.explode(F.array(*[F.lit(k) for k in range(-tol, tol + 1)])))
               .withColumn("price_date", F.expr(f"date_add(p_date, {days} + off)")))
    closest = Window.partitionBy(*keys, "price_date").orderBy(F.abs("off"))
    prior = (prior.withColumn("rn", F.row_number().over(closest)).filter("rn = 1")
                  .select(*keys, "price_date", "p_val"))
    joined = df.join(prior, keys + ["price_date"], "left")
    return joined.withColumn(out, F.when(F.col("p_val") > 0, F.round((F.col(col) / F.col("p_val") - 1) * 100, 4))).drop("p_val")


def in_scope(df):
    if start_date:
        df = df.filter(F.col("price_date") >= F.lit(start_date).cast("date"))
    if end_date:
        df = df.filter(F.col("price_date") <= F.lit(end_date).cast("date"))
    return df


def merge_gold(name, df, keys):
    """MERGE df into the Gold table. Returns (rows in df, inserted, updated)."""
    df = df.withColumn("load_timestamp", F.current_timestamp())
    df.createOrReplaceTempView("gold_incoming")
    rows_out = spark.table("gold_incoming").count()
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in df.columns if c not in keys and c != "load_timestamp")
    res = spark.sql(f"""
        MERGE INTO {G(name)} AS t
        USING gold_incoming AS s
        ON {on}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    return rows_out, int(res["num_inserted_rows"]), int(res["num_updated_rows"])

# COMMAND ----------

# Read Silver once
silver = spark.table(SILVER_TABLE)
rows_read = silver.count()
base = silver.select("price_date", "city_code", "item_no", "min_price", "avg_price", "max_price", "is_available")

# ---- dimensions ----
dim_date = (silver.select(F.col("price_date").alias("week_ending_date")).distinct()
            .withColumn("date_key", F.date_format("week_ending_date", "yyyyMMdd").cast("int"))
            .withColumn("cal_year", F.year("week_ending_date"))
            .withColumn("cal_quarter", F.quarter("week_ending_date"))
            .withColumn("cal_month", F.month("week_ending_date"))
            .withColumn("month_name", F.date_format("week_ending_date", "MMMM"))
            .withColumn("iso_week", F.weekofyear("week_ending_date"))
            .withColumn("year_month", F.date_format("week_ending_date", "yyyy-MM"))
            .select("date_key", "week_ending_date", "cal_year", "cal_quarter", "cal_month",
                    "month_name", "iso_week", "year_month"))

dim_city = (silver.groupBy("city_code").agg(F.max("city_name").alias("city_name"))
            .withColumnRenamed("city_code", "city_key"))

# Food / non-food: a keyword rule on the item name. Check the printed list below and fix any item in OVERRIDES.
NON_FOOD = "electric|energy saver|telephone|gas|lpg|petrol|diesel|kerosene|firewood|wood|coal|soap|detergent|match|cigarette|sandal|chappal|shoe|cloth|lawn|georgette|shirting|chiffon|cambric|khaddar"
OVERRIDES = {}          # example: {44: "Non-food"}
category = F.when(F.lower("item_name").rlike(NON_FOOD), "Non-food").otherwise("Food")
for no, cat in OVERRIDES.items():
    category = F.when(F.col("item_no") == no, F.lit(cat)).otherwise(category)

dim_item = (silver.groupBy("item_no").agg(F.max("item_name").alias("item_name"), F.max("unit").alias("unit"))
            .withColumn("category", category)
            .withColumnRenamed("item_no", "item_key"))

# ---- fact: one row per week, city, item ----
fact = with_prior(base, ["city_code", "item_no"], "avg_price", 7, "wow_pct")
fact = with_prior(fact, ["city_code", "item_no"], "avg_price", 364, "yoy_pct")
fact = (fact.withColumn("date_key", date_key())
            .withColumnRenamed("city_code", "city_key").withColumnRenamed("item_no", "item_key")
            .select("price_date", "date_key", "city_key", "item_key", "min_price", "avg_price", "max_price",
                    "is_available", "wow_pct", "yoy_pct"))

# ---- city price index (fixed basket, base = first week) ----
first_week = base.agg(F.min("price_date")).collect()[0][0]
base_prices = (base.filter((F.col("price_date") == F.lit(first_week)) & (F.col("avg_price") > 0))
                   .select("city_code", "item_no", F.col("avg_price").alias("base_price")))
city_index = (base.filter(F.col("avg_price").isNotNull()).join(base_prices, ["city_code", "item_no"])
              .withColumn("relative", F.col("avg_price") / F.col("base_price") * 100)
              .groupBy("price_date", "city_code")
              .agg(F.round(F.avg("relative"), 4).alias("price_index"), F.count("*").cast("int").alias("items_in_index")))
city_index = with_prior(city_index, ["city_code"], "price_index", 7, "wow_pct")
city_index = with_prior(city_index, ["city_code"], "price_index", 28, "pct_4w")
city_index = with_prior(city_index, ["city_code"], "price_index", 364, "yoy_pct")

# ---- national index = average of the city indexes ----
national = (city_index.groupBy("price_date").agg(F.round(F.avg("price_index"), 4).alias("price_index")))
national = with_prior(national, [], "price_index", 7, "wow_pct")
national = with_prior(national, [], "price_index", 364, "yoy_pct")

# ---- item volatility over the last 12 weeks of data ----
item_week = (base.filter(F.col("avg_price").isNotNull()).groupBy("price_date", "item_no")
                 .agg(F.round(F.avg("avg_price"), 4).alias("national_avg_price")))
item_week = with_prior(item_week, ["item_no"], "national_avg_price", 7, "wow_pct")
last12 = Window.partitionBy("item_no").orderBy("price_date").rowsBetween(-11, 0)
volatility = (item_week
              .withColumn("rolling_std_12w", F.round(F.stddev("wow_pct").over(last12), 4))
              .withColumn("max_swing_12w", F.round(F.max(F.abs("wow_pct")).over(last12), 4))
              .withColumn("weeks_in_window", F.count("wow_pct").over(last12).cast("int")))

# COMMAND ----------

# Load every table. One table failing is logged and the next one still runs; the notebook fails at the end.
steps = [
    ("dim_date",   dim_date,   ["date_key"],                        False),
    ("dim_city",   dim_city,   ["city_key"],                        False),
    ("dim_item",   dim_item,   ["item_key"],                        False),
    ("fact_spi_price", fact,   ["date_key", "city_key", "item_key"], True),
    ("gold_city_weekly_index", city_index.withColumn("date_key", date_key())
        .withColumnRenamed("city_code", "city_key"), ["date_key", "city_key"], True),
    ("gold_national_weekly_index", national.withColumn("date_key", date_key()), ["date_key"], True),
    ("gold_item_volatility", volatility.withColumn("date_key", date_key())
        .withColumnRenamed("item_no", "item_key"), ["date_key", "item_key"], True),
]

failed = []
for name, df, keys, scoped in steps:
    started = datetime.now()
    parameter = f"{name}; start={start_date or '-'}, end={end_date or '-'}"
    try:
        if scoped:
            df = in_scope(df)
        cols = [f.name for f in spark.table(G(name)).schema.fields if f.name != "load_timestamp"]
        rows_out, inserted, updated = merge_gold(name, df.select(*cols), keys)
        write_log(batch_id, "Silver-to-Gold", run_mode, parameter, started, "Success", inserted, updated, 0,
                  f"rows_read_from_silver={rows_read}; rows_in_output={rows_out}")
        print(f"OK   {name}: read={rows_read} output={rows_out} inserted={inserted} updated={updated}")
    except Exception as e:
        write_log(batch_id, "Silver-to-Gold", run_mode, parameter, started, "Failure", 0, 0, 0, str(e)[:1000])
        print(f"FAIL {name}: {e}")
        failed.append(name)

# COMMAND ----------

# Gold checks: no repeated key, no fact row pointing at a missing dimension row.
started = datetime.now()
dupes = spark.sql(f"""SELECT COUNT(*) FROM (SELECT 1 FROM {G('fact_spi_price')}
                      GROUP BY date_key, city_key, item_key HAVING COUNT(*) > 1)""").collect()[0][0]
orphans = spark.sql(f"""SELECT COUNT(*) FROM {G('fact_spi_price')} f
                        LEFT JOIN {G('dim_date')} d ON f.date_key = d.date_key
                        LEFT JOIN {G('dim_city')} c ON f.city_key = c.city_key
                        LEFT JOIN {G('dim_item')} i ON f.item_key = i.item_key
                        WHERE d.date_key IS NULL OR c.city_key IS NULL OR i.item_key IS NULL""").collect()[0][0]
short = spark.sql(f"""SELECT date_key, COUNT(*) AS n FROM {G('fact_spi_price')}
                      GROUP BY date_key HAVING COUNT(*) <> {EXPECTED_CITIES * EXPECTED_ITEMS}""").collect()
ok = dupes == 0 and orphans == 0
msg = f"duplicate_keys={dupes}; orphan_fact_rows={orphans}"
if short:        # a warning only: a week with missing rows (quarantined in Silver) is possible, but should be looked at
    msg += "; WARNING weeks without " + str(EXPECTED_CITIES * EXPECTED_ITEMS) + " fact rows: " + ", ".join(f"{r.date_key}={r.n}" for r in short)[:700]
write_log(batch_id, "Silver-to-Gold", run_mode, "gold checks", started, "Success" if ok else "Failure", 0, 0, 0, msg)
print(f"checks: duplicate keys={dupes}, orphan fact rows={orphans}, incomplete weeks={len(short)}")
if not ok:
    failed.append("gold checks")

# COMMAND ----------

# Read optimization: compact files and apply the clustering on the large tables.
for name in ("fact_spi_price", "gold_city_weekly_index"):
    try:
        spark.sql(f"OPTIMIZE {G(name)}")
    except Exception as e:
        print(f"OPTIMIZE skipped for {name}: {e}")

if failed:
    raise RuntimeError(f"Silver-to-Gold failed for: {failed}")

# COMMAND ----------

# Check the food / non-food tags and fix any wrong item in OVERRIDES (above), then run again.
display(spark.table(G("dim_item")).orderBy("item_key"))

# COMMAND ----------

display(spark.table(LOG_TABLE).filter(F.col("batch_id") == batch_id).orderBy("start_time"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fact rows per week: every week should show 867
# MAGIC SELECT d.week_ending_date, COUNT(*) AS fact_rows
# MAGIC FROM workspace.spi.fact_spi_price f
# MAGIC JOIN workspace.spi.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.week_ending_date
# MAGIC ORDER BY d.week_ending_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spi.fact_spi_price order by date_key desc