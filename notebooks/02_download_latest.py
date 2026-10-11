# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01b_download_latest
# MAGIC Weekly job step: finds the newest SPI annexure(s) on pbs.gov.pk and saves them in the raw folders.
# MAGIC No browser and no CSV needed. Safe to run any number of times: files already saved are skipped,
# MAGIC and a week that is not published yet is reported, not treated as an error.
# MAGIC
# MAGIC How the newest files are found
# MAGIC 1. The latest week's links are in the plain HTML of the price-statistics page. Links with `annex` in the file name are used.
# MAGIC 2. For the last `weeks_back` Thursdays the usual file name `Annex_DD.MM.YYYY.xlsx` / `.pdf` is also tried, so a missed week is caught up.
# MAGIC
# MAGIC Rule (same as `01_download_annexures`): xlsx if the week has one, otherwise pdf.
# MAGIC
# MAGIC **Parameters**
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `weeks_back` | How many recent Thursdays to check. Default 4. |
# MAGIC | `upload_base` | Folder URL used for the file-name guesses. |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
import re
import time
from datetime import date, timedelta
from urllib.parse import urljoin, urlparse, unquote

import requests

dbutils.widgets.text("weeks_back", "4")
dbutils.widgets.text("upload_base", "https://www.pbs.gov.pk/wp-content/uploads/2020/07")

weeks_back = int(dbutils.widgets.get("weeks_back").strip() or "4")
upload_base = dbutils.widgets.get("upload_base").strip().rstrip("/")

PAGE_URL = "https://www.pbs.gov.pk/price-statistics/"
XLSX_DIR, PDF_DIR = RAW_FOLDER, RAW_PDF_FOLDER
for d in (XLSX_DIR, PDF_DIR):
    os.makedirs(d, exist_ok=True)

session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0"

# COMMAND ----------

def norm_url(u):
    return re.sub(r"(?<!:)/{2,}", "/", u.strip())


def url_file(u):
    return unquote(urlparse(u).path.split("/")[-1])


def name_date(fname):
    """Date inside a file name as YYYY-MM-DD, or None."""
    m = re.search(r"(\d{2})[.\-](\d{2})[.\-](\d{4})", fname) or re.search(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)", fname)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def valid(kind, body):
    return body[:2] == b"PK" if kind == "xlsx" else b"%PDF" in body[:1024]


def fetch(url):
    """File bytes, or None if the file is not there (404) or not a real file."""
    for attempt in range(3):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 404:
                return None
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        time.sleep(1 + attempt * 2)
    return None

# COMMAND ----------

# 1) Links found on the page itself (the newest week).
try:
    html = session.get(PAGE_URL, timeout=60).text
except requests.RequestException as e:
    raise RuntimeError(f"Cannot reach pbs.gov.pk from this compute ({type(e).__name__}).")

found = {}      # week (YYYY-MM-DD) -> {"xlsx": url, "pdf": url}
for href in re.findall(r"""href=["']([^"']+?\.(?:xlsx|pdf))(?:\?[^"']*)?["']""", html, re.I):
    u = norm_url(urljoin(PAGE_URL, href))
    f = url_file(u).lower()
    wk = name_date(f)
    if wk and "annex" in f and "summ" not in f:
        found.setdefault(wk, {}).setdefault(f.rsplit(".", 1)[-1], u)

# 2) Usual file names for the last few Thursdays (catches a week the job missed).
today = date.today()
last_thu = today - timedelta(days=(today.weekday() - 3) % 7)
for i in range(weeks_back):
    wk = last_thu - timedelta(weeks=i)
    for kind in ("xlsx", "pdf"):
        found.setdefault(wk.isoformat(), {}).setdefault(kind, f"{upload_base}/Annex_{wk:%d.%m.%Y}.{kind}")

print(f"weeks to check: {len(found)}")

# COMMAND ----------

downloaded, present, missing = [], [], []

for wk in sorted(found, reverse=True):
    y, m, d = wk.split("-")
    stem = f"Annex_{d}.{m}.{y}"
    if os.path.exists(f"{XLSX_DIR}/{stem}.xlsx") or os.path.exists(f"{PDF_DIR}/{stem}.pdf"):
        present.append(wk)
        continue
    saved = False
    for kind in ("xlsx", "pdf"):                                   # xlsx first, pdf only if there is no xlsx
        url = found[wk].get(kind)
        body = fetch(url) if url else None
        if body and valid(kind, body):
            with open(f"{XLSX_DIR if kind == 'xlsx' else PDF_DIR}/{stem}.{kind}", "wb") as f:
                f.write(body)
            downloaded.append(f"{wk} ({kind})")
            saved = True
            break
    if not saved:
        missing.append(wk)

print("downloaded:", downloaded or "nothing new")
print("already in raw:", present)
print("not published or not found:", missing)

# COMMAND ----------

