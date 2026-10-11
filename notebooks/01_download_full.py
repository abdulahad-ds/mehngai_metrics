# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01_download_annexures
# MAGIC Downloads the weekly SPI annexures listed in `spi_weeks_flags.csv` (made by `pbs_spi_scraper.py` on a PC).
# MAGIC Rule: if a week has an Excel annexure, only the xlsx is saved. If not, the pdf annexure is saved.
# MAGIC Output folders: `<RAW_FOLDER>/xlsx` and `<RAW_FOLDER>/pdf`. Files are named `Annex_DD.MM.YYYY.<ext>` from the week-ended date.
# MAGIC Upload the CSV to `<RAW_FOLDER>/manifest/spi_weeks_flags.csv` before running the download cells.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
import re
import time
from urllib.parse import urlparse, unquote

import requests

XLSX_DIR = f"{RAW_FOLDER}/xlsx"
PDF_DIR = f"{RAW_FOLDER}/pdf"
MANIFEST_DIR = f"{RAW_FOLDER}/manifest"
MANIFEST_PATH = f"{MANIFEST_DIR}/spi_weeks_flags.csv"

for d in (XLSX_DIR, PDF_DIR, MANIFEST_DIR):
    os.makedirs(d, exist_ok=True)

if not os.path.exists(MANIFEST_PATH):
    raise FileNotFoundError(f"Upload spi_weeks_flags.csv to {MANIFEST_PATH} and run this cell again.")

manifest = pd.read_csv(MANIFEST_PATH, dtype=str).fillna("")
print(f"weeks in manifest: {len(manifest)}")

# COMMAND ----------

def norm_url(u):
    """Remove doubled slashes in the path (https://host//wp-content/... -> https://host/wp-content/...)."""
    return re.sub(r"(?<!:)/{2,}", "/", u.strip())


def url_file(u):
    return unquote(urlparse(u).path.split("/")[-1])


def name_date(fname):
    """Date inside a file name as YYYY-MM-DD, or None (handles 24.09.2026, 24-09-2026 and 24092026)."""
    m = re.search(r"(\d{2})[.\-](\d{2})[.\-](\d{4})", fname) or re.search(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)", fname)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def candidates(row):
    """Links allowed for this week, xlsx first. A link is rejected if its file name has no 'annex'
    (it is a summary/report) or if the date in the file name is a different week."""
    ok, notes = [], []
    for kind, col in (("xlsx", "annexure_xlsx_link"), ("pdf", "annexure_pdf_link")):
        u = row[col].strip()
        if not u:
            continue
        u = norm_url(u)
        f = url_file(u)
        if "annex" not in f.lower():
            notes.append(f"{kind}: no 'annex' in file name ({f})")
            continue
        d = name_date(f)
        if d and d != row["week_ended"]:
            notes.append(f"{kind}: file date {d} is a different week ({f})")
            continue
        ok.append((kind, u))
    return ok, notes


def valid(kind, body):
    if kind == "xlsx":
        return body[:2] == b"PK"
    return b"%PDF" in body[:1024]


session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0"


def fetch(url):
    for attempt in range(3):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        time.sleep(1 + attempt * 2)
    return None

# COMMAND ----------

# Connection check: stops early with a clear message if this compute cannot reach pbs.gov.pk.
first = next(u for _, row in manifest.iterrows() for _, u in candidates(row)[0])
try:
    r = session.get(first, timeout=30)
    print("connection check:", r.status_code, first)
except requests.RequestException as e:
    raise RuntimeError(f"Cannot reach pbs.gov.pk from this compute ({type(e).__name__}). Downloads will not work here.")

# COMMAND ----------

counts = {"xlsx_saved": 0, "pdf_saved": 0, "already_present": 0}
skipped, failed = [], []

for _, row in manifest.iterrows():
    week = row["week_ended"]
    if not week:
        skipped.append(("(no date)", "week_ended is empty"))
        continue
    cands, notes = candidates(row)
    if not cands:
        skipped.append((week, "; ".join(notes) or "no annexure link"))
        continue

    stem = "Annex_" + "{2}.{1}.{0}".format(*week.split("-"))      # 2026-09-24 -> Annex_24.09.2026
    done = False
    for kind, url in cands:                                      # xlsx first, pdf only if xlsx fails
        dest = f"{XLSX_DIR if kind == 'xlsx' else PDF_DIR}/{stem}.{kind}"
        if os.path.exists(dest):
            counts["already_present"] += 1
            done = True
            break
        body = fetch(url)
        if body and valid(kind, body):
            with open(dest, "wb") as f:
                f.write(body)
            counts[f"{kind}_saved"] += 1
            done = True
            break
    if not done:
        failed.append(week)

print(counts)
print("skipped (no usable annexure link):")
for w, why in skipped:
    print(" ", w, "-", why)
print("failed downloads:", failed)