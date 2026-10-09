# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # parse_annexure
# MAGIC Reads sheet `Appendix-A` of one PBS weekly Annexure Excel file and returns one row per (item, city).
# MAGIC Values stay as text on purpose. Cleaning and casting happen later, in Silver.

# COMMAND ----------

import re
import pandas as pd

CITY_RE = re.compile(r"^\s*(.+?)\s*\((\d{1,2})\)\s*$")      # example: Islamabad (01)
DATE_RE = re.compile(r"PRICES ON\s+(\d{2}-\d{2}-\d{4})", re.I)  # example: PRICES ON 24-09-2026


def clean(v):
    """Empty cell -> None, anything else -> trimmed text."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v).strip()


def parse_appendix_a(path):
    df = pd.read_excel(path, sheet_name="Appendix-A", header=None, dtype=str)
    out = []
    cities = {}      
    price_date = None

    for _, row in df.iterrows():
        cells = [clean(v) for v in row.values]

        # 1) a city header row: it has cells like "Lahore (05)"
        found = {}
        for col, v in enumerate(cells):
            if v is not None and CITY_RE.match(v):
                found[col] = v
        if found:
            cities = found
            continue

        # 2) a date row: "PRICES ON 24-09-2026"
        for v in cells:
            if v is not None:
                m = DATE_RE.search(v)
                if m:
                    price_date = m.group(1)

        # 3) an item row: first cell is a number, second cell is a name (not a number)
        sl, name, unit = cells[0], cells[1], cells[2]
        if sl is None or name is None or not sl.isdigit() or name.isdigit():
            continue
        for start, city in cities.items():
            out.append({
                "source_file": path.split("/")[-1],
                "price_date_text": price_date,
                "item_sl_no": sl,
                "item_description": name,
                "unit": unit,
                "city_raw": city,
                "min_price_raw": cells[start] if start < len(cells) else None,
                "avg_price_raw": cells[start + 1] if start + 1 < len(cells) else None,
                "max_price_raw": cells[start + 2] if start + 2 < len(cells) else None,
            })
    return out

# COMMAND ----------

# CHECK: run this on one file and send me the printed result
p = "/Volumes/workspace/spi/raw/Annex_24.09.2026 (3).xlsx"
rows = parse_appendix_a(p)
pdf = pd.DataFrame(rows)
print("rows:", len(pdf))
print("cities:", pdf.city_raw.nunique(), "| items:", pdf.item_description.nunique())
print("date:", pdf.price_date_text.unique())
print(pdf.city_raw.unique())
print(pdf.head(5).to_string())

# COMMAND ----------



# COMMAND ----------

