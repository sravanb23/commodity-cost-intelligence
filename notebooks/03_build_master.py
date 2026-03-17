# =============================================================================
# 03: BUILD MASTER DATASET
# Assembles final analytical dataset from validated warehouse files
# Dependencies: warehouse_summary.json must exist and show HEALTHY status
# Output: data/processed/commodity_master.csv
# Schema: date | aluminum_usd_mt | lithium_usd_mt | cobalt_usd_mt |
#         wti_crude_usd | natural_gas_usd | industrial_production_idx |
#         vix | usd_cny | year | month | quarter
# =============================================================================

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

os.makedirs("data/processed", exist_ok=True)

print("=" * 60)
print("03: BUILD MASTER DATASET")
print("=" * 60)

# =============================================================================
# STEP 1: CHECK WAREHOUSE HEALTH BEFORE BUILDING
# =============================================================================
print("\nStep 1: Checking warehouse health...")

summary_path = "data/processed/warehouse_summary.json"
if not os.path.exists(summary_path):
    raise FileNotFoundError(
        "warehouse_summary.json not found. Run 02_build_warehouse.py first."
    )

with open(summary_path, "r") as f:
    summary = json.load(f)

print(f"  Warehouse status : {summary['status']}")
print(f"  Last validated   : {summary['last_validated']}")

if summary["issues"]:
    print(f"  ⚠️  {len(summary['issues'])} warehouse issue(s):")
    for issue in summary["issues"]:
        print(f"     — {issue}")
    print(f"\n  Proceeding with build — review issues above")
else:
    print(f"  ✅ Warehouse healthy — proceeding with master build")

# =============================================================================
# STEP 2: LOAD PRIMARY COMMODITY SERIES
# =============================================================================
print("\nStep 2: Loading commodity series...")

# Aluminum — World Bank monthly
aluminum = pd.read_csv(
    "data/warehouse/aluminum_worldbank.csv",
    parse_dates=["date"]
)[["date", "value"]].rename(columns={"value": "aluminum_usd_mt"})

print(f"  ✅ Aluminum  : {len(aluminum)} rows | "
      f"{aluminum['date'].min().date()} → {aluminum['date'].max().date()}")

# Lithium — Combined USGS+IMF spliced series
lithium = pd.read_csv(
    "data/warehouse/lithium_combined.csv",
    parse_dates=["date"]
)[["date", "value"]].rename(columns={"value": "lithium_usd_mt"})

print(f"  ✅ Lithium   : {len(lithium)} rows | "
      f"{lithium['date'].min().date()} → {lithium['date'].max().date()}")

# Cobalt — IMF direct monthly USD
cobalt = pd.read_csv(
    "data/warehouse/cobalt_imf_monthly.csv",
    parse_dates=["date"]
)[["date", "value"]].rename(columns={"value": "cobalt_usd_mt"})

print(f"  ✅ Cobalt    : {len(cobalt)} rows | "
      f"{cobalt['date'].min().date()} → {cobalt['date'].max().date()}")

# =============================================================================
# STEP 3: LOAD MACRO VARIABLES
# =============================================================================
print("\nStep 3: Loading macro variables...")

macro = pd.read_csv(
    "data/warehouse/macro_fred.csv",
    parse_dates=["date"]
)

print(f"  ✅ Macro FRED: {len(macro)} rows | "
      f"{macro['date'].min().date()} → {macro['date'].max().date()}")
print(f"     Columns: {[c for c in macro.columns if c != 'date']}")

vix = pd.read_csv(
    "data/warehouse/vix_yahoo.csv",
    parse_dates=["date"]
)[["date", "value"]].rename(columns={"value": "vix"})

print(f"  ✅ VIX Yahoo : {len(vix)} rows | "
      f"{vix['date'].min().date()} → {vix['date'].max().date()}")

# =============================================================================
# STEP 4: BUILD DATE SPINE
# Use aluminum as the primary date spine (most complete monthly series)
# =============================================================================
print("\nStep 4: Building master date spine...")

master = aluminum.copy()
print(f"  Date spine: {len(master)} months from aluminum series")

# =============================================================================
# STEP 5: MERGE ALL SERIES
# =============================================================================
print("\nStep 5: Merging all series...")

# Merge commodities
master = master.merge(lithium, on="date", how="left")
master = master.merge(cobalt,  on="date", how="left")

# Merge macro — select only the value columns we need
macro_cols = ["date", "wti_crude_usd", "natural_gas_usd",
              "industrial_production_idx", "usd_cny"]
macro_slim = macro[[c for c in macro_cols if c in macro.columns]]
master = master.merge(macro_slim, on="date", how="left")

# Merge VIX
master = master.merge(vix, on="date", how="left")

# =============================================================================
# STEP 6: ADD TIME DIMENSIONS
# =============================================================================
print("\nStep 6: Adding time dimensions...")

master = master.sort_values("date").reset_index(drop=True)
master["year"]    = master["date"].dt.year
master["month"]   = master["date"].dt.month
master["quarter"] = master["date"].dt.quarter

# =============================================================================
# STEP 7: VALIDATE MASTER DATASET
# =============================================================================
print("\nStep 7: Validating master dataset...")

print(f"\n  Shape: {master.shape[0]} rows × {master.shape[1]} columns")
print(f"  Range: {master['date'].min().date()} → "
      f"{master['date'].max().date()}")
print(f"\n  Missing values per column:")

commodity_cols = [
    "aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt",
    "wti_crude_usd", "natural_gas_usd",
    "industrial_production_idx", "usd_cny", "vix"
]

all_clean = True
for col in commodity_cols:
    if col not in master.columns:
        print(f"    ⚠️  {col:<35}: COLUMN MISSING")
        all_clean = False
        continue
    missing = master[col].isna().sum()
    pct     = missing / len(master) * 100
    status  = "✅" if missing == 0 else "⚠️ "
    print(f"    {status} {col:<35}: {missing:>4} missing ({pct:.1f}%)")
    if missing > 0:
        all_clean = False

print(f"\n  Latest values (most recent row):")
latest = master.iloc[-1]
print(f"    Date               : {latest['date'].date()}")
for col in commodity_cols:
    if col in master.columns and pd.notna(latest[col]):
        print(f"    {col:<35}: {latest[col]:>12.2f}")

if all_clean:
    print(f"\n  ✅ Master dataset clean — no missing values")
else:
    print(f"\n  ⚠️  Some missing values present — see above")

# =============================================================================
# STEP 8: SAVE MASTER DATASET
# =============================================================================
print("\nStep 8: Saving master dataset...")

output_path = "data/processed/commodity_master.csv"
master.to_csv(output_path, index=False)

print(f"  ✅ Saved: {output_path}")
print(f"     {master.shape[0]} rows × {master.shape[1]} columns")

# =============================================================================
# STEP 9: SAVE MASTER BUILD METADATA
# =============================================================================
print("\nStep 9: Saving master build metadata...")

lineage_path = "data/processed/data_lineage.json"
try:
    with open(lineage_path, "r") as f:
        lineage = json.load(f)
except FileNotFoundError:
    lineage = {}

lineage["master"] = {
    "last_built":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "output_path":      output_path,
    "rows":             master.shape[0],
    "columns":          master.shape[1],
    "date_range_start": str(master["date"].min().date()),
    "date_range_end":   str(master["date"].max().date()),
    "commodity_sources": {
        "aluminum": "data/warehouse/aluminum_worldbank.csv",
        "lithium":  "data/warehouse/lithium_combined.csv "
                    "(USGS pre-2012-06 + IMF post-2012-06)",
        "cobalt":   "data/warehouse/cobalt_imf_monthly.csv"
    },
    "macro_sources": {
        "fred": "data/warehouse/macro_fred.csv",
        "vix":  "data/warehouse/vix_yahoo.csv"
    },
    "missing_values": {
        col: int(master[col].isna().sum())
        for col in commodity_cols
        if col in master.columns
    }
}

with open(lineage_path, "w") as f:
    json.dump(lineage, f, indent=2)

print(f"  ✅ Data lineage updated: {lineage_path}")

# =============================================================================
# FINAL PREVIEW
# =============================================================================
print("\n" + "=" * 60)
print("MASTER DATASET PREVIEW (first 3 rows):")
print(master.head(3).to_string())
print("\nMASTER DATASET PREVIEW (last 3 rows):")
print(master.tail(3).to_string())
print("=" * 60)
print(f"03 COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)
