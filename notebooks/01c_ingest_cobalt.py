# =============================================================================
# 01c: COBALT INGESTION
# Sources:
#   - USGS Mineral Commodity Summaries (annual, reference only)
#   - IMF PCPS Cobalt Monthly USD prices (direct monthly spot prices)
# Method:
#   Real monthly USD prices available directly from IMF dataset
#   No interpolation or index reconstruction needed
# Outputs:
#   data/warehouse/cobalt_usgs.csv        (USGS annual, reference only)
#   data/warehouse/cobalt_imf_monthly.csv (IMF direct monthly USD prices)
# Schema: date | value | unit | source | frequency | notes
# =============================================================================

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# --- Dependency check ---------------------------------------------------------
USGS_PATH = "data/raw/usgs_cobalt.xlsx"
IMF_PATH  = "data/raw/imf_commodity_prices.csv"
os.makedirs("data/warehouse", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

for path in [USGS_PATH, IMF_PATH]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file not found: {path}")

print("=" * 60)
print("01c: COBALT INGESTION")
print("=" * 60)

# =============================================================================
# STEP 1: LOAD USGS ANNUAL COBALT PRICES (REFERENCE ONLY)
# =============================================================================
print("\nStep 1: Loading USGS annual cobalt prices (reference)...")

usgs_raw = pd.read_excel(USGS_PATH, skiprows=4, engine="openpyxl")

price_col = [
    c for c in usgs_raw.columns
    if "unit value" in str(c).lower()
    and "98$" not in str(c).lower()
][0]

print(f"  Price column: '{price_col}'")

usgs = usgs_raw[["Year", price_col]].copy()
usgs.columns = ["year", "value"]
usgs["year"]  = pd.to_numeric(usgs["year"],  errors="coerce")
usgs["value"] = pd.to_numeric(usgs["value"], errors="coerce")
usgs = usgs.dropna()
usgs["year"] = usgs["year"].astype(int)
usgs = usgs[usgs["year"] >= 2000].sort_values("year").reset_index(drop=True)

print(f"  ✅ USGS annual: {len(usgs)} observations")
print(f"     Range: {usgs['year'].min()} → {usgs['year'].max()}")
print(f"\n  Raw USGS values ($/mt):")
print(usgs.to_string(index=False))

# Save USGS reference table
usgs_warehouse = pd.DataFrame({
    "date":      pd.to_datetime(usgs["year"].astype(str) + "-01-01"),
    "value":     usgs["value"].round(2),
    "unit":      "USD per metric ton",
    "source":    "USGS Mineral Commodity Summaries",
    "frequency": "Annual",
    "notes":     "Reference only — IMF monthly used as primary"
})
usgs_warehouse.to_csv("data/warehouse/cobalt_usgs.csv", index=False)
print(f"\n  ✅ Saved reference: data/warehouse/cobalt_usgs.csv")

# =============================================================================
# STEP 2: LOAD IMF DIRECT MONTHLY COBALT USD PRICES
# Row: "Cobalt, US dollars per pound" | "US dollars" | "Monthly"
# =============================================================================
print("\nStep 2: Loading IMF direct monthly cobalt USD prices...")

imf_raw = pd.read_csv(IMF_PATH)

cobalt_monthly_row = imf_raw[
    (imf_raw["INDICATOR"].str.contains("Cobalt", na=False)) &
    (imf_raw["OBS_MEASURE"] == "OBS_VALUE") &
    (imf_raw["DATA_TRANSFORMATION"] == "US dollars") &
    (imf_raw["FREQUENCY"] == "Monthly")
]

print(f"  IMF cobalt monthly USD rows found: {len(cobalt_monthly_row)}")

if len(cobalt_monthly_row) == 0:
    raise ValueError("IMF cobalt monthly USD row not found — check dataset")

# Extract monthly columns
monthly_cols = [c for c in imf_raw.columns if "-M" in str(c)]

cobalt_long = cobalt_monthly_row[monthly_cols].T.reset_index()
cobalt_long.columns = ["period", "value"]
cobalt_long = cobalt_long[cobalt_long["value"] != ""]
cobalt_long["value"] = pd.to_numeric(cobalt_long["value"], errors="coerce")
cobalt_long["date"]  = pd.to_datetime(
    cobalt_long["period"].str.replace("-M", "-", regex=False),
    format="%Y-%m", errors="coerce"
)
cobalt_long = cobalt_long.dropna(subset=["date", "value"])
cobalt_long = cobalt_long[["date", "value"]].sort_values("date").reset_index(drop=True)
cobalt_long = cobalt_long[cobalt_long["date"] >= "2000-01-01"]

print(f"  ✅ IMF monthly cobalt: {len(cobalt_long)} observations")
print(f"     Range: {cobalt_long['date'].min().date()} → "
      f"{cobalt_long['date'].max().date()}")

# =============================================================================
# STEP 3: VALIDATE AGAINST USGS ANNUAL (SPOT CHECK)
# =============================================================================
print("\nStep 3: Validating IMF monthly against USGS annual...")

cobalt_long["year"] = cobalt_long["date"].dt.year
imf_annual_avg = (
    cobalt_long.groupby("year")["value"]
    .mean()
    .reset_index()
    .rename(columns={"value": "imf_annual_avg"})
)

comparison = usgs.merge(imf_annual_avg, on="year", how="inner")
comparison["diff_pct"] = (
    (comparison["imf_annual_avg"] - comparison["value"])
    / comparison["value"] * 100
)

print(f"\n  {'Year':>6} {'USGS USD':>12} {'IMF Avg USD':>14} {'Diff %':>10}")
print(f"  {'-'*6} {'-'*12} {'-'*14} {'-'*10}")
for _, row in comparison.iterrows():
    print(f"  {int(row['year']):>6} "
          f"${row['value']:>11,.0f} "
          f"${row['imf_annual_avg']:>13,.0f} "
          f"{row['diff_pct']:>+9.1f}%")

# Clean up year column
cobalt_long = cobalt_long.drop(columns=["year"])

# =============================================================================
# STEP 4: FINAL VALIDATION
# =============================================================================
print("\nStep 4: Final validation...")

missing = cobalt_long["value"].isna().sum()
print(f"  Missing values : {missing}")
print(f"  Min price      : ${cobalt_long['value'].min():>10,.2f}/mt")
print(f"  Max price      : ${cobalt_long['value'].max():>10,.2f}/mt")
print(f"  Latest price   : ${cobalt_long['value'].iloc[-1]:>10,.2f}/mt "
      f"({cobalt_long['date'].iloc[-1].date()})")

print(f"\n  Sample — 2017 (known spike year):")
spike = cobalt_long[cobalt_long["date"].dt.year == 2017][["date", "value"]]
print(spike.to_string(index=False))

if missing > 0:
    print(f"\n  ⚠️  {missing} missing values — review pipeline")
else:
    print(f"\n  ✅ No missing values")

# =============================================================================
# STEP 5: BUILD AND SAVE WAREHOUSE TABLE
# =============================================================================
print("\nStep 5: Saving to warehouse...")

cobalt_warehouse = pd.DataFrame({
    "date":      cobalt_long["date"],
    "value":     cobalt_long["value"].round(2),
    "unit":      "USD per metric ton",
    "source":    "IMF Primary Commodity Price System (PCPS)",
    "frequency": "Monthly",
    "notes":     "Direct monthly USD spot prices — no interpolation or reconstruction"
})

cobalt_warehouse.to_csv("data/warehouse/cobalt_imf_monthly.csv", index=False)
print(f"  ✅ Saved: data/warehouse/cobalt_imf_monthly.csv")
print(f"     {len(cobalt_warehouse)} rows x "
      f"{len(cobalt_warehouse.columns)} columns")

# =============================================================================
# STEP 6: UPDATE DATA LINEAGE
# =============================================================================
print("\nStep 6: Updating data lineage...")

lineage_path = "data/processed/data_lineage.json"

try:
    with open(lineage_path, "r") as f:
        lineage = json.load(f)
except FileNotFoundError:
    lineage = {}

lineage["cobalt"] = {
    "last_updated":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "primary_source":   "IMF Primary Commodity Price System (PCPS)",
    "reference_source": "USGS Mineral Commodity Summaries",
    "unit":             "USD per metric ton",
    "method":           "Direct monthly USD spot prices — no transformation needed",
    "frequency":        "Monthly",
    "total_rows":       len(cobalt_warehouse),
    "date_range_start": str(cobalt_long["date"].min().date()),
    "date_range_end":   str(cobalt_long["date"].max().date()),
    "warehouse_files": {
        "imf_monthly":    "data/warehouse/cobalt_imf_monthly.csv",
        "usgs_reference": "data/warehouse/cobalt_usgs.csv"
    }
}

with open(lineage_path, "w") as f:
    json.dump(lineage, f, indent=2)

print(f"  ✅ Data lineage saved: {lineage_path}")

print("\n" + "=" * 60)
print(f"01c COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)