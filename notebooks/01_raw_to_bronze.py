# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # 01_raw_to_bronze
# MAGIC Reads the weekly Annexure Excel files and merges them into the Bronze table.
# MAGIC
# MAGIC **Parameters (widgets at the top of the notebook)**
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `run_mode` | `incremental` = skip files that already loaded successfully. `full` = process every file in range again (safe, because of MERGE). |
# MAGIC | `raw_folder` | Folder that holds the .xlsx files. |
# MAGIC | `start_date`, `end_date` | Optional `YYYY-MM-DD`. Only files whose name has a date in this range are used. Empty = no limit. |
# MAGIC | `batch_id` | Optional label for this run. Empty = a new one is generated. |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("run_mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("raw_folder", RAW_FOLDER)
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("batch_id", "")

run_mode = dbutils.widgets.get("run_mode")
raw_folder = dbutils.widgets.get("raw_folder").rstrip("/")
start_date = dbutils.widgets.get("start_date").strip()
end_date = dbutils.widgets.get("end_date").strip()
batch_id = dbutils.widgets.get("batch_id").strip() or str(uuid.uuid4())

lo = date.fromisoformat(start_date) if start_date else None
hi = date.fromisoformat(end_date) if end_date else None
print("run_mode:", run_mode, "| folder:", raw_folder, "| range:", start_date or "-", "to", end_date or "-", "| batch:", batch_id)

ensure_tables()

# COMMAND ----------

# 1) Which files will we process?
names = sorted(f.name for f in dbutils.fs.ls(raw_folder) if f.name.lower().endswith(".xlsx"))

def in_range(name):
    d = date_from_filename(name)
    if d is None:
        return True          # no date in the name: we cannot filter it, so keep it
    return (lo is None or d >= lo) and (hi is None or d <= hi)

names = [n for n in names if in_range(n)]

if run_mode == "incremental":
    done = {r.parameter for r in
            spark.table(LOG_TABLE)
                 .filter((F.col("layer") == "Raw-to-Bronze") & (F.col("status") == "Success"))
                 .select("parameter").collect()}
    names = [n for n in names if n not in done]

print(f"{len(names)} file(s) to process")

# COMMAND ----------

# 2) Process one file at a time. One bad file never stops the others.
def merge_bronze(df):
    df.createOrReplaceTempView("bronze_incoming")
    res = spark.sql(f"""
        MERGE INTO {BRONZE_TABLE} AS t
        USING bronze_incoming AS s
        ON  t.source_file = s.source_file
        AND t.item_sl_no  = s.item_sl_no
        AND t.city_raw    = s.city_raw
        WHEN MATCHED AND NOT (
                t.price_date_text    <=> s.price_date_text
            AND t.item_description   <=> s.item_description
            AND t.unit               <=> s.unit
            AND t.min_price_raw      <=> s.min_price_raw
            AND t.avg_price_raw      <=> s.avg_price_raw
            AND t.max_price_raw      <=> s.max_price_raw)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    return int(res["num_inserted_rows"]), int(res["num_updated_rows"])


for name in names:
    started = datetime.now()
    try:
        rows = parse_appendix_a(f"{raw_folder}/{name}")
        df = (spark.createDataFrame(rows, BRONZE_PARSE_SCHEMA)          # explicit schema
                   .withColumn("batch_id", F.lit(batch_id))
                   .withColumn("load_timestamp", F.current_timestamp())
                   .select([f.name for f in BRONZE_SCHEMA.fields])       # same column order as the table
                   .dropDuplicates(["source_file", "item_sl_no", "city_raw"]))  # MERGE needs one source row per key
        inserted, updated = merge_bronze(df)

        n_cities = len({r[5] for r in rows})
        n_items = len({r[2] for r in rows})
        warn = None
        if n_cities != EXPECTED_CITIES or n_items != EXPECTED_ITEMS:
            warn = f"WARNING: found {n_cities} cities and {n_items} items (expected {EXPECTED_CITIES} and {EXPECTED_ITEMS})"

        write_log(batch_id, "Raw-to-Bronze", run_mode, name, started, "Success", inserted, updated, 0, warn)
        print(f"OK   {name}: inserted={inserted} updated={updated} {warn or ''}")

    except Exception as e:
        write_log(batch_id, "Raw-to-Bronze", run_mode, name, started, "Failure", 0, 0, 0, str(e)[:1000])
        print(f"FAIL {name}: {e}")
        try:   # keep a copy of the bad file so we can look at it later
            dbutils.fs.cp(f"{raw_folder}/{name}", f"{raw_folder}/quarantine/{name}")
        except Exception:
            pass

# COMMAND ----------

display(spark.table(LOG_TABLE).filter(F.col("batch_id") == batch_id).orderBy("start_time"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.spi.bronze_spi_prices WHERE item_sl_no = 23 ORDER BY avg_price_raw;

# COMMAND ----------

