# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_config
# MAGIC Shared settings used by every notebook: table names, explicit schemas, the Excel parser, and the logging function.
# MAGIC Other notebooks load this one with `%run ./00_config`.

# COMMAND ----------

import re
import uuid
from datetime import datetime, date

import pandas as pd
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, TimestampType, LongType, DateType,
)

# ---- names and default paths (change only here) ----
CATALOG = "workspace"
SCHEMA = "spi"
RAW_FOLDER = "/Volumes/workspace/spi/raw"          # default folder with the Annexure .xlsx files

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_spi_prices"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_spi_prices"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_spi_quarantine"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_execution_logs"

EXPECTED_CITIES = 17
EXPECTED_ITEMS = 51

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit schemas 

# COMMAND ----------

# Bronze: everything the parser reads stays as text. Only the two metadata columns are typed.
BRONZE_DATA_FIELDS = [
    StructField("source_file", StringType(), False),
    StructField("price_date_text", StringType(), True),
    StructField("item_sl_no", StringType(), False),
    StructField("item_description", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("city_raw", StringType(), False),
    StructField("min_price_raw", StringType(), True),
    StructField("avg_price_raw", StringType(), True),
    StructField("max_price_raw", StringType(), True),
]
BRONZE_PARSE_SCHEMA = StructType(BRONZE_DATA_FIELDS)     # what we read from the Excel file
BRONZE_SCHEMA = StructType(
    [StructField("batch_id", StringType(), False)] + BRONZE_DATA_FIELDS +
    [StructField("load_timestamp", TimestampType(), False)]
)

SILVER_SCHEMA = StructType([
    StructField("price_date", DateType(), False),
    StructField("city_code", IntegerType(), False),
    StructField("city_name", StringType(), True),
    StructField("item_no", IntegerType(), False),
    StructField("item_name", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("min_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("max_price", DoubleType(), True),
    StructField("is_available", BooleanType(), False),
    StructField("source_file", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])

QUARANTINE_SCHEMA = StructType([
    StructField("batch_id", StringType(), True),
    StructField("source_file", StringType(), False),
    StructField("price_date_text", StringType(), True),
    StructField("item_sl_no", StringType(), False),
    StructField("item_description", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("city_raw", StringType(), False),
    StructField("min_price_raw", StringType(), True),
    StructField("avg_price_raw", StringType(), True),
    StructField("max_price_raw", StringType(), True),
    StructField("reject_reason", StringType(), True),
    StructField("load_timestamp", TimestampType(), False),
])

LOG_DATA_FIELDS = [
    StructField("log_id", StringType(), False),
    StructField("batch_id", StringType(), True),
    StructField("layer", StringType(), False),            # Raw-to-Bronze or Bronze-to-Silver
    StructField("run_mode", StringType(), True),          # full or incremental
    StructField("parameter", StringType(), True),         # file name, or the date range
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),            # Success or Failure
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("message", StringType(), True),
]
LOG_SCHEMA = StructType(LOG_DATA_FIELDS + [StructField("load_timestamp", TimestampType(), False)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the tables (safe to run many times)

# COMMAND ----------

def ensure_tables():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    for name, schema in [(BRONZE_TABLE, BRONZE_SCHEMA), (SILVER_TABLE, SILVER_SCHEMA),
                         (QUARANTINE_TABLE, QUARANTINE_SCHEMA), (LOG_TABLE, LOG_SCHEMA)]:
        # mode("ignore") = do nothing if the table already exists
        spark.createDataFrame([], schema).write.format("delta").mode("ignore").saveAsTable(name)

# Let MERGE add new columns by itself if the source ever grows one (schema evolution).
try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
except Exception as e:
    print("Could not set autoMerge on this compute:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel parser (one row per item and city, values kept as text)

# COMMAND ----------

CITY_RE = re.compile(r"^\s*(.+?)\s*\((\d{1,2})\)\s*$")          # Islamabad (01)
DATE_RE = re.compile(r"PRICES ON\s+(\d{2}-\d{2}-\d{4})", re.I)   # PRICES ON 24-09-2026
FILE_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")          # Annex_24.09.2026.xlsx


def clean(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v).strip()


def date_from_filename(name):
    """Annex_24.09.2026.xlsx -> date(2026, 9, 24). Returns None if the name has no date."""
    m = FILE_DATE_RE.search(name)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def parse_appendix_a(path):
    xl = pd.ExcelFile(path)
    if "Appendix-A" not in xl.sheet_names:
        raise ValueError(f"Sheet 'Appendix-A' not found. Sheets: {xl.sheet_names}")
    df = pd.read_excel(path, sheet_name="Appendix-A", header=None, dtype=str)
    out, cities, price_date = [], {}, None
    for _, row in df.iterrows():
        cells = [clean(v) for v in row.values]
        found = {c: v for c, v in enumerate(cells) if v is not None and CITY_RE.match(v)}
        if found:
            cities = found
            continue
        for v in cells:
            if v is not None:
                m = DATE_RE.search(v)
                if m:
                    price_date = m.group(1)
        sl, name, unit = cells[0], cells[1], cells[2]
        if sl is None or name is None or not sl.isdigit() or name.isdigit():
            continue
        for start, city in cities.items():
            out.append((
                path.split("/")[-1], price_date, sl, name, unit, city,
                cells[start] if start < len(cells) else None,
                cells[start + 1] if start + 1 < len(cells) else None,
                cells[start + 2] if start + 2 < len(cells) else None,
            ))
    if not out:
        raise ValueError("No item rows found. The Excel layout may have changed.")
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging function (one row in pipeline_execution_logs)

# COMMAND ----------

def write_log(batch_id, layer, run_mode, parameter, start_time, status,
              inserted=0, updated=0, quarantined=0, message=None):
    row = [(str(uuid.uuid4()), batch_id, layer, run_mode, parameter, start_time,
            datetime.now(), status, int(inserted), int(updated), int(quarantined), message)]
    (spark.createDataFrame(row, StructType(LOG_DATA_FIELDS))
          .withColumn("load_timestamp", F.current_timestamp())
          .write.mode("append").saveAsTable(LOG_TABLE))