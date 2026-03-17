# =============================================================================
# 01a: ALUMINUM INGESTION
# Source: World Bank Pink Sheet (Monthly Prices)
# Output: data/warehouse/aluminum_worldbank.csv
# Schema: date | value | unit | source | frequency | notes
# =============================================================================

import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- Dependency check ---------------------------------------------------------
RAW_FALLBACK  = "data/raw/aluminum_raw.csv"
OUTPUT_PATH   = "data/warehouse/aluminum_worldbank.csv"
os.makedirs("data/warehouse", exist_ok=True)

WORLD_BANK_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "18675f1d1639c7a34d463f59263ba0a2-0050012025"
    "/related/CMO-Historical-Data-Monthly.xlsx"
)

print("=" * 60)
print("01a: ALUMINUM INGESTION")
print("=" * 60)

# --- Step 1: Download World Bank Pink Sheet -----------------------------------
print("\nStep 1: Downloading World Bank Pink Sheet...")

try:
    wb_raw = pd.read_excel(
        WORLD_BANK_URL,
        sheet_name="Monthly Prices",
        skiprows=4,
        engine="openpyxl"
    )
    print(f"  ✅ Downloaded: {wb_raw.shape[0]} rows, {wb_raw.shape[1]} columns")
    source_note = "World Bank Pink Sheet (live download)"

except Exception as e:
    print(f"  ⚠️  Live download failed: {e}")
    print(f"  Falling back to: {RAW_FALLBACK}")

    if not os.path.exists(RAW_FALLBACK):
        raise FileNotFoundError(
            f"Live download failed and fallback not found at {RAW_FALLBACK}"
        )

    wb_raw = pd.read_csv(RAW_FALLBACK)
    source_note = "World Bank Pink Sheet (cached fallback)"
    print(f"  ✅ Fallback loaded: {wb_raw.shape[0]} rows")

# --- Step 2: Extract aluminum column -----------------------------------------
print("\nStep 2: Extracting aluminum prices...")

date_col = wb_raw.columns[0]
alum_col = [c for c in wb_raw.columns if "Aluminum" in str(c)][0]
print(f"  Date column : {date_col}")
print(f"  Price column: {alum_col}")

aluminum = wb_raw[[date_col, alum_col]].copy()
aluminum.columns = ["raw_date", "value"]

# Drop units row (first row contains "$/mt")
aluminum = aluminum.iloc[1:].copy()
aluminum = aluminum.dropna(subset=["raw_date"])

# --- Step 3: Parse World Bank date format (1960M01 → datetime) ---------------
print("\nStep 3: Parsing dates...")

aluminum["raw_date"] = aluminum["raw_date"].astype(str).str.strip()
aluminum["date"] = pd.to_datetime(
    aluminum["raw_date"].str.replace("M", "-", regex=False),
    format="%Y-%m",
    errors="coerce"
)
aluminum = aluminum.dropna(subset=["date"])
aluminum["value"] = pd.to_numeric(aluminum["value"], errors="coerce")
aluminum = aluminum.dropna(subset=["value"])

# Filter to 2000 onwards
aluminum = aluminum[aluminum["date"] >= "2000-01-01"].reset_index(drop=True)

print(f"  ✅ Parsed: {len(aluminum)} monthly observations")
print(f"     Range: {aluminum['date'].min().date()} → "
      f"{aluminum['date'].max().date()}")

# --- Step 4: Build warehouse schema ------------------------------------------
print("\nStep 4: Building warehouse table...")

warehouse = pd.DataFrame({
    "date":      aluminum["date"],
    "value":     aluminum["value"].round(2),
    "unit":      "USD per metric ton",
    "source":    source_note,
    "frequency": "Monthly",
    "notes":     "LME aluminum spot price, standard grade"
})

# --- Step 5: Validate ---------------------------------------------------------
print("\nStep 5: Validating...")

missing = warehouse["value"].isna().sum()
print(f"  Missing values : {missing}")
print(f"  Min price      : ${warehouse['value'].min():,.2f}/mt")
print(f"  Max price      : ${warehouse['value'].max():,.2f}/mt")
print(f"  Latest price   : ${warehouse['value'].iloc[-1]:,.2f}/mt "
      f"({warehouse['date'].iloc[-1].date()})")

if missing > 0:
    print(f"  ⚠️  {missing} missing values detected — review raw data")
else:
    print(f"  ✅ No missing values")

# --- Step 6: Save -------------------------------------------------------------
print("\nStep 6: Saving to warehouse...")

warehouse.to_csv(OUTPUT_PATH, index=False)
print(f"  ✅ Saved: {OUTPUT_PATH}")
print(f"     {len(warehouse)} rows × {len(warehouse.columns)} columns")

print("\n" + "=" * 60)
print(f"01a COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)