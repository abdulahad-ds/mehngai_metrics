# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_config
# MAGIC Shared settings used by every notebook: table names, explicit schemas, the Excel and PDF parsers, and the logging function.
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
RAW_FOLDER = "/Volumes/workspace/spi/raw/xlsx"          # default folder with the Annexure .xlsx files
RAW_PDF_FOLDER = "/Volumes/workspace/spi/raw/pdf"  # default folder with the Annexure .pdf files (change to your real folder)

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
FILE_DATE_RE = re.compile(r"(\d{2})[._-](\d{2})[._-](\d{4})")      # Annex_24.09.2026.xlsx or Annex-09.10.2025-1.pdf


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
# MAGIC ## PDF parser (same output as the Excel parser)
# MAGIC The Appendix-A PDF has 3 pages of the same table: cities 1-7, cities 8-14, and cities 15-17
# MAGIC (the last page also has National Average and week/year comparison columns, which are ignored).
# MAGIC The summary tables after Appendix-A (fertilizer, cement, CNG, wages, wheat) are skipped.

# COMMAND ----------

try:
    import pdfplumber
except ImportError:                      # works inside %run too (a %pip magic would not)
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pdfplumber"])
    import pdfplumber

# a price cell, or a "not available" marker
_NUM_RE = re.compile(r"^(?:-?\d+(?:\.\d+)?|N\.?A\.?|-{1,2})$", re.I)

# "Islamabad (01)" inside a header line (may be several cities on one line)
# (one word per city name: header words from neighbouring columns can land right before it in the text)
_PDF_CITY_RE = re.compile(r"([A-Za-z][A-Za-z.\-]*)\s*\((\d{2})\)")

# The text of a row is "<description><unit>" with no clean gap, so we recognise the unit at the end.
_UNIT_RE = re.compile(
    r"(Bag \d+(?:\.\d+)? ?Kg"                       # Bag 20 Kg
    r"|Each|Pair|MMBTU"
    r"|Per (?:Unit|Litre|Plate|Cup|Minute)"
    r"|\d+(?:\.\d+)? ?(?:Kg|Ltr|Dozen|mtr|gm))$",   # 1 Kg, 1 Ltr, 1 Dozen, 1 mtr, 40 Kg
    re.I,
)


def _split_desc_unit(text):
    m = _UNIT_RE.search(text)
    if not m:
        return text.strip(), None
    return text[:m.start()].strip(), re.sub(r"\s+", " ", m.group(1)).strip()


def parse_pdf_page_text(text, file_name, price_date=None):
    """
    Parse the text of ONE Appendix-A page.
    Returns (rows, price_date). Rows have the same shape as parse_appendix_a():
    (source_file, price_date_text, item_sl_no, item_description, unit, city_raw, min, avg, max)
    """
    lines = [re.sub(r"\s+", " ", l).strip() for l in text.splitlines()]
    lines = [l for l in lines if l]

    # pass 1: this page's city list (in column order) and the date
    cities = []
    for l in lines:
        m = DATE_RE.search(l)
        if m:
            price_date = m.group(1)
        if not l[0].isdigit():
            cities += [f"{n.strip()} ({c})" for n, c in _PDF_CITY_RE.findall(l)]
    if not cities:
        return [], price_date            # not an Appendix-A page (e.g. the summary tables)

    # pass 2: item rows look like   <sl> <description> <unit> <price x 3 per city ...> <sl>
    rows = []
    for l in lines:
        t = l.split(" ")
        if len(t) < 5 or not t[0].isdigit() or t[-1] != t[0]:
            continue                     # summary-table rows do not end with the serial number again
        body = t[1:-1]
        k = len(body)
        while k > 0 and _NUM_RE.match(body[k - 1]):
            k -= 1
        nums, label = body[k:], " ".join(body[:k])
        if not label or len(nums) < 3 * len(cities):
            continue
        desc, unit = _split_desc_unit(label)
        for i, city in enumerate(cities):
            rows.append((file_name, price_date, t[0], desc, unit, city,
                         nums[3 * i], nums[3 * i + 1], nums[3 * i + 2]))
    return rows, price_date


def parse_appendix_a_pdf(path):
    file_name = path.split("/")[-1]
    out, price_date = [], None
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            rows, price_date = parse_pdf_page_text(page.extract_text(use_text_flow=True, x_tolerance=1.5) or "", file_name, price_date)
            out.extend(rows)
    if not out:
        raise ValueError("No item rows found in the PDF. The layout may have changed or it may be a scanned image.")
    expected = EXPECTED_CITIES * EXPECTED_ITEMS
    if len(out) != expected:
        print(f"WARNING {file_name}: parsed {len(out)} rows, expected {expected}")
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## One entry point for both file types
# MAGIC The ingest notebook should call `parse_file(path)` instead of calling the Excel parser directly.

# COMMAND ----------

def parse_file(path):
    ext = path.lower().rsplit(".", 1)[-1]
    if ext in ("xlsx", "xls"):
        rows = parse_appendix_a(path)
    elif ext == "pdf":
        rows = parse_appendix_a_pdf(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")

    # The date inside the file and the date in the file name (Annex_24.09.2026.xlsx) must agree. Silver is keyed on the
    # price date, so a file published under the wrong week would otherwise silently overwrite that other week.
    fd = date_from_filename(path.split("/")[-1])
    if fd is not None:
        fallback = fd.strftime("%d-%m-%Y")
        in_file = {r[1] for r in rows if r[1]}
        wrong = {d for d in in_file if d != fallback}
        if wrong:
            raise ValueError(f"Date mismatch: file name says {fallback} but the file contains {sorted(wrong)}")
        # If the date was not found inside the file, use the date in the file name
        rows = [(r[0], r[1] or fallback) + r[2:] for r in rows]
    return rows

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

# COMMAND ----------

