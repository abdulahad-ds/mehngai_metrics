# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02_bronze_to_silver
# MAGIC Cleans and casts Bronze rows, sends bad rows to the quarantine table, and merges good rows into Silver.
# MAGIC
# MAGIC **Parameters**
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `start_date`, `end_date` | Optional `YYYY-MM-DD`. Only Bronze rows with a price date in this range are processed. Empty = all. |
# MAGIC | `source_file` | Optional. Process only this one Bronze file name. |
# MAGIC | `run_mode` | `incremental` or `full`. Used for the log. Both are safe to repeat because of MERGE. |
# MAGIC | `batch_id` | Optional label for this run. |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("run_mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("batch_id", "")

run_mode = dbutils.widgets.get("run_mode")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
source_file = dbutils.widgets.get("source_file").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or str(uuid.uuid4())
parameter = f"start={start_date or '-'}, end={end_date or '-'}, source_file={source_file or '-'}"
print(parameter, "| batch:", batch_id)

ensure_tables()

# COMMAND ----------

PRICE_DATE = F.expr("cast(try_to_timestamp(price_date_text, 'dd-MM-yyyy') as date)")   # NULL if not a date
MISSING_MARKERS = ["N.A.", "NA", "N/A", "-", ""]    # text PBS uses when a price is not available

def item_reference():
    """One clean name and unit per item_no, taken from the most recent week (xlsx first)."""
    a = (spark.table(BRONZE_TABLE)
         .withColumn("price_date", PRICE_DATE)
         .withColumn("item_no", F.expr("try_cast(item_sl_no as int)"))
         .filter(F.col("price_date").isNotNull() & F.col("item_no").isNotNull()))
    w = Window.partitionBy("item_no").orderBy(
        F.col("price_date").desc(), F.col("source_file").endswith(".xlsx").desc(),
        F.col("load_timestamp").desc())
    return (a.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
             .select("item_no",
                     F.regexp_replace(F.regexp_replace(F.trim("item_description"), r"[\s*]+$", ""),
                                      r"\s+", " ").alias("ref_name"),
                     F.trim("unit").alias("ref_unit")))
    
def build_silver(bronze):
    """Returns (good_rows, rejected_rows) as two DataFrames."""
    b = (bronze
         .withColumnRenamed("load_timestamp", "bronze_load_ts")
         .withColumnRenamed("batch_id", "bronze_batch_id")
         .withColumn("price_date", PRICE_DATE)
         .withColumn("city_name", F.regexp_extract("city_raw", r"^(.*?)\s*\((\d+)\)\s*$", 1))
         .withColumn("city_code_txt", F.regexp_extract("city_raw", r"^(.*?)\s*\((\d+)\)\s*$", 2))
         # try_cast gives NULL instead of crashing when the text is not a number
         .withColumn("city_code", F.expr("try_cast(city_code_txt as int)"))
         .withColumn("item_no", F.expr("try_cast(item_sl_no as int)"))
         .withColumn("min_v", F.expr("try_cast(min_price_raw as double)"))
         .withColumn("avg_v", F.expr("try_cast(avg_price_raw as double)"))
         .withColumn("max_v", F.expr("try_cast(max_price_raw as double)")))

    def is_marker(c):    # empty or one of the "not available" texts
        return F.col(c).isNull() | F.upper(F.trim(F.col(c))).isin(MISSING_MARKERS)

    def is_garbage(raw, val):    # has text, is not a marker, but is not a number
        return (~is_marker(raw)) & F.col(val).isNull()

    all_zero = (F.col("min_v") == 0) & (F.col("avg_v") == 0) & (F.col("max_v") == 0)
    all_marker = is_marker("min_price_raw") & is_marker("avg_price_raw") & is_marker("max_price_raw")
    not_available = F.coalesce(all_zero, F.lit(False)) | all_marker

    bad_number = (is_garbage("min_price_raw", "min_v") | is_garbage("avg_price_raw", "avg_v")
                  | is_garbage("max_price_raw", "max_v"))
    negative = F.coalesce((F.col("min_v") < 0) | (F.col("avg_v") < 0) | (F.col("max_v") < 0), F.lit(False))
    out_of_order = F.coalesce((F.col("min_v") > F.col("avg_v")) | (F.col("avg_v") > F.col("max_v")), F.lit(False))

    reason = (F.when(F.col("price_date").isNull(), "bad date")
               .when(F.col("city_code").isNull(), "bad city code")
               .when(F.col("item_no").isNull(), "bad item number")
               .when(bad_number, "non-numeric price")
               .when(negative, "negative price")
               .when((~not_available) & out_of_order, "min/avg/max out of order"))
    b = b.withColumn("reject_reason", reason).withColumn("not_available", not_available)

    rejected = b.filter(F.col("reject_reason").isNotNull()).select(
        F.col("bronze_batch_id").alias("batch_id"), "source_file", "price_date_text", "item_sl_no",
        "item_description", "unit", "city_raw", "min_price_raw", "avg_price_raw", "max_price_raw",
        "reject_reason", F.current_timestamp().alias("load_timestamp"))

    # If the same (date, city, item) arrives from two files, keep the newest one.
    w = (Window.partitionBy("price_date", "city_code", "item_no")
               .orderBy(F.col("bronze_load_ts").desc(), F.col("source_file").desc()))
    good = (b.filter(F.col("reject_reason").isNull())
             .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
             .select(
                 "price_date", "city_code", "city_name", "item_no",
                 F.col("item_description").alias("item_name"), "unit",
                 F.when(F.col("not_available"), F.lit(None).cast("double")).otherwise(F.col("min_v")).alias("min_price"),
                 F.when(F.col("not_available"), F.lit(None).cast("double")).otherwise(F.col("avg_v")).alias("avg_price"),
                 F.when(F.col("not_available"), F.lit(None).cast("double")).otherwise(F.col("max_v")).alias("max_price"),
                 (~F.col("not_available")).alias("is_available"),
                 "source_file", F.lit(batch_id).alias("batch_id"),
                 F.current_timestamp().alias("load_timestamp")))
    good = (good.join(F.broadcast(item_reference()), "item_no", "left")
            .withColumn("item_name", F.coalesce("ref_name", "item_name"))
            .withColumn("unit", F.coalesce("ref_unit", "unit"))
            .select([f.name for f in SILVER_SCHEMA.fields]))
    return good, rejected

# COMMAND ----------

started = datetime.now()
try:
    bronze = spark.table(BRONZE_TABLE)
    if start_date:
        bronze = bronze.filter(PRICE_DATE >= F.lit(start_date).cast("date"))
    if end_date:
        bronze = bronze.filter(PRICE_DATE <= F.lit(end_date).cast("date"))
    if source_file:
        bronze = bronze.filter(F.col("source_file") == source_file)

    good, rejected = build_silver(bronze)
    good.createOrReplaceTempView("silver_incoming")
    rejected.createOrReplaceTempView("quarantine_incoming")

    # --- MERGE good rows into Silver ---
    res = spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS t
        USING silver_incoming AS s
        ON  t.price_date = s.price_date AND t.city_code = s.city_code AND t.item_no = s.item_no
        WHEN MATCHED AND NOT (
                t.city_name    <=> s.city_name
            AND t.item_name    <=> s.item_name
            AND t.unit         <=> s.unit
            AND t.min_price    <=> s.min_price
            AND t.avg_price    <=> s.avg_price
            AND t.max_price    <=> s.max_price
            AND t.is_available <=> s.is_available)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    inserted, updated = int(res["num_inserted_rows"]), int(res["num_updated_rows"])

    # --- MERGE rejected rows into the quarantine table (so reruns do not duplicate them) ---
    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING quarantine_incoming AS s
        ON  t.source_file = s.source_file AND t.item_sl_no = s.item_sl_no AND t.city_raw = s.city_raw
        WHEN MATCHED AND NOT (t.reject_reason <=> s.reject_reason
                              AND t.min_price_raw <=> s.min_price_raw
                              AND t.avg_price_raw <=> s.avg_price_raw
                              AND t.max_price_raw <=> s.max_price_raw)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    quarantined = rejected.count()

    write_log(batch_id, "Bronze-to-Silver", run_mode, parameter, started, "Success", inserted, updated, quarantined)
    print(f"OK: inserted={inserted} updated={updated} quarantined={quarantined}")

except Exception as e:
    write_log(batch_id, "Bronze-to-Silver", run_mode, parameter, started, "Failure", 0, 0, 0, str(e)[:1000])
    raise

# COMMAND ----------

display(spark.table(LOG_TABLE).filter(F.col("batch_id") == batch_id))