# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Loan Tape Generator
# MAGIC Writes one CSV per month into a Unity Catalog Volume.
# MAGIC Edit the settings in the next cell to match your catalog / schema.

# COMMAND ----------

import os
import csv
import random
from datetime import date

# ---- Settings: change these to match what you created ----
CATALOG = "workspace"      # or "loan_demo"
SCHEMA = "loan_tape"       # or "tape"
VOLUME = "landing"
N_LOANS = 1000
SNAPSHOTS = [date(2025, 1, 31), date(2025, 2, 28), date(2025, 3, 31)]
# -----------------------------------------------------------

random.seed(42)
OUT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
os.makedirs(OUT_DIR, exist_ok=True)

STATES = ["CA", "TX", "NY", "FL", "IL", "WA", "OH", "GA", "NC", "PA"]
PRODUCTS = {
    "Personal":   (3000, 30000, 9, 24),    # min bal, max bal, min rate, max rate
    "Auto":       (8000, 45000, 5, 14),
    "HomeEquity": (20000, 150000, 6, 12),
}

# COMMAND ----------

# Build the static loan master (attributes that never change)
loans = []
for i in range(N_LOANS):
    product = random.choice(list(PRODUCTS))
    lo, hi, rmin, rmax = PRODUCTS[product]
    orig_year = random.choice([2022, 2023, 2024])
    orig_month = random.randint(1, 12)
    loans.append({
        "loan_id": f"LN{100000 + i}",
        "origination_date": date(orig_year, orig_month, 1),
        "original_balance": round(random.uniform(lo, hi), 2),
        "interest_rate": round(random.uniform(rmin, rmax), 2),
        "term_months": random.choice([24, 36, 48, 60]),
        "credit_score": random.randint(580, 820),
        "state": random.choice(STATES),
        "product_type": product,
        "balance": None,   # filled on first snapshot
        "dpd": 0,
    })


def months_between(d1, d2):
    return (d2.year - d1.year) * 12 + (d2.month - d1.month)


def next_dpd(dpd):
    """Simple delinquency transition model."""
    r = random.random()
    if dpd == 0:
        return 30 if r < 0.04 else 0
    if dpd == 30:
        return 0 if r < 0.45 else (60 if r < 0.80 else 30)
    if dpd == 60:
        return 0 if r < 0.25 else (90 if r < 0.70 else 60)
    # 90+
    return 0 if r < 0.08 else min(dpd + 30, 180)


def status_for(dpd, balance):
    if balance <= 0:
        return "Paid Off"
    if dpd == 0:
        return "Current"
    if dpd < 90:
        return "Delinquent"
    if dpd >= 150:
        return "Default"
    return "Seriously Delinquent"

# COMMAND ----------

HEADER = [
    "loan_id", "snapshot_date", "origination_date", "original_balance",
    "current_balance", "interest_rate", "term_months", "days_past_due",
    "loan_status", "state", "credit_score", "product_type",
]

for idx, snap in enumerate(SNAPSHOTS):
    rows = []
    for ln in loans:
        elapsed = months_between(ln["origination_date"], snap)
        if elapsed < 0 or elapsed > ln["term_months"]:
            continue  # not yet originated, or matured

        monthly_principal = ln["original_balance"] / ln["term_months"]
        if ln["balance"] is None:
            ln["balance"] = max(0.0, ln["original_balance"] - monthly_principal * elapsed)
        else:
            ln["balance"] = max(0.0, ln["balance"] - monthly_principal)
            ln["dpd"] = next_dpd(ln["dpd"])

        if ln["balance"] <= 0:
            ln["dpd"] = 0

        rows.append([
            ln["loan_id"], snap.isoformat(), ln["origination_date"].isoformat(),
            ln["original_balance"], round(ln["balance"], 2), ln["interest_rate"],
            ln["term_months"], ln["dpd"], status_for(ln["dpd"], ln["balance"]),
            ln["state"], ln["credit_score"], ln["product_type"],
        ])

    # ---- Inject dirty rows for data quality practice (~1% each) ----
    n_bad = max(2, len(rows) // 100)
    for _ in range(n_bad):
        bad = list(random.choice(rows))
        bad[0] = ""                       # null loan_id
        rows.append(bad)
    for _ in range(n_bad):
        bad = list(random.choice(rows))
        bad[4] = -abs(float(bad[4]) or 100)   # negative balance
        rows.append(bad)
    for _ in range(n_bad):
        bad = list(random.choice(rows))
        bad[5] = 999                      # impossible interest rate
        rows.append(bad)
    for _ in range(n_bad):
        rows.append(list(random.choice(rows)))  # exact duplicate

    random.shuffle(rows)

    path = f"{OUT_DIR}/loan_tape_{snap.strftime('%Y_%m')}.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"Wrote {len(rows):>5} rows -> {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the files landed

# COMMAND ----------

display(dbutils.fs.ls(OUT_DIR))

# COMMAND ----------

# Peek at one file
df = spark.read.csv(f"{OUT_DIR}/loan_tape_2025_01.csv", header=True)
display(df.limit(10))