# =============================================================================
# 01d: MACRO VARIABLES INGESTION
# Sources:
#   - FRED API: WTI Crude Oil, Natural Gas, Industrial Production, USD/CNY FX
#   - Yahoo Finance: VIX (market uncertainty)
# Outputs:
#   data/warehouse/macro_fred.csv   (all FRED series combined)
#   data/warehouse/vix_yahoo.csv    (VIX from Yahoo Finance)
# Schema: date | value | unit | source | frequency | notes
# =============================================================================

import pandas as pd
import numpy as np
import yfinance as yf
import os
import json
from fredapi import Fred
from datetime import datetime

# --- Dependency check ---------------------------------------------------------
os.makedirs("data/warehouse", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

FRED_API_KEY = "ac39f6d4a1fe61821bfb838c11abe140"
START_DATE   = "2000-01-01"
END_DATE     = "2026-12-31"

print("=" * 60)
print("01d: MACRO VARIABLES INGESTION")
print("=" * 60)

fred = Fred(api_key=FRED_API_KEY)

# =============================================================================
# FRED SERIES DEFINITIONS
# =============================================================================
FRED_SERIES = {
    "DCOILWTICO": {
        "col":   "wti_crude_usd",
        "unit":  "USD per barrel",
        "notes": "WTI crude oil spot price — energy cost proxy "
                 "for aluminum smelting"
    },
    "MHHNGSP": {
        "col":   "natural_gas_usd",
        "unit":  "USD per million BTU",
        "notes": "Henry Hub natural gas spot price — "
                 "smelting and processing energy cost"
    },
    "IPMAN": {
        "col":   "industrial_production_idx",
        "unit":  "Index (2017=100)",
        "notes": "US Industrial Production Manufacturing Index — "
                 "commodity demand proxy"
    },
    "DEXCHUS": {
        "col":   "usd_cny",
        "unit":  "CNY per USD",
        "notes": "USD/CNY exchange rate — China commodity "
                 "demand and export competitiveness proxy"
    },
}

# =============================================================================
# STEP 1: PULL ALL FRED SERIES
# =============================================================================
print("\nStep 1: Pulling FRED series...")

fred_frames = []

for series_id, meta in FRED_SERIES.items():
    try:
        raw = fred.get_series(
            series_id,
            observation_start=START_DATE,
            observation_end=END_DATE
        )

        df = raw.reset_index()
        df.columns = ["date", "value"]
        df["date"]  = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Resample to monthly average
        df = (
            df.set_index("date")
              .resample("MS")
              .mean()
              .reset_index()
        )

        # Add metadata columns
        df["series_id"] = series_id
        df["column"]    = meta["col"]
        df["unit"]      = meta["unit"]
        df["source"]    = f"FRED — {series_id}"
        df["notes"]     = meta["notes"]

        fred_frames.append(df)

        print(f"  ✅ {series_id:12s} ({meta['col']:30s}): "
              f"{len(df)} rows | "
              f"{df['date'].min().date()} → {df['date'].max().date()} | "
              f"Missing: {df['value'].isna().sum()}")

    except Exception as e:
        print(f"  ❌ {series_id}: {e}")

# =============================================================================
# STEP 2: BUILD WIDE-FORMAT MACRO TABLE
# =============================================================================
print("\nStep 2: Building wide-format macro table...")

# Start with date spine from first series
date_spine = fred_frames[0][["date"]].copy()

macro_wide = date_spine.copy()

for df in fred_frames:
    col_name = df["column"].iloc[0]
    col_df   = df[["date", "value"]].rename(columns={"value": col_name})
    macro_wide = macro_wide.merge(col_df, on="date", how="outer")

macro_wide = macro_wide.sort_values("date").reset_index(drop=True)
macro_wide = macro_wide[macro_wide["date"] >= START_DATE]

print(f"  ✅ Wide macro table: {macro_wide.shape[0]} rows × "
      f"{macro_wide.shape[1]} columns")
print(f"     Range: {macro_wide['date'].min().date()} → "
      f"{macro_wide['date'].max().date()}")
print(f"\n  Missing values per series:")
for col in ["wti_crude_usd", "natural_gas_usd",
            "industrial_production_idx", "usd_cny"]:
    print(f"    {col:35s}: {macro_wide[col].isna().sum()}")

# =============================================================================
# STEP 3: VALIDATE FRED DATA
# =============================================================================
print("\nStep 3: Validating FRED data...")

for col, meta in [
    ("wti_crude_usd",            "WTI Crude ($/bbl)"),
    ("natural_gas_usd",          "Natural Gas ($/mmBTU)"),
    ("industrial_production_idx","Industrial Production (idx)"),
    ("usd_cny",                  "USD/CNY FX"),
]:
    series = macro_wide[col].dropna()
    print(f"  {meta:35s} | "
          f"Min: {series.min():>8.2f} | "
          f"Max: {series.max():>8.2f} | "
          f"Latest: {series.iloc[-1]:>8.2f} "
          f"({macro_wide.loc[macro_wide[col].notna(), 'date'].iloc[-1].date()})")

# =============================================================================
# STEP 4: SAVE FRED MACRO TABLE
# =============================================================================
print("\nStep 4: Saving FRED macro table...")

macro_wide.to_csv("data/warehouse/macro_fred.csv", index=False)
print(f"  ✅ Saved: data/warehouse/macro_fred.csv")
print(f"     {macro_wide.shape[0]} rows × {macro_wide.shape[1]} columns")

# =============================================================================
# STEP 5: PULL VIX FROM YAHOO FINANCE
# =============================================================================
print("\nStep 5: Pulling VIX from Yahoo Finance...")

try:
    vix_raw = yf.download(
        "^VIX",
        start=START_DATE,
        end=END_DATE,
        progress=False
    )

    vix_df = (
        vix_raw[["Close"]]
        .resample("MS")
        .mean()
        .reset_index()
    )
    vix_df.columns = ["date", "value"]
    vix_df["date"]  = pd.to_datetime(vix_df["date"])
    vix_df["value"] = pd.to_numeric(vix_df["value"], errors="coerce")
    vix_df = vix_df[vix_df["date"] >= START_DATE].reset_index(drop=True)

    print(f"  ✅ VIX: {len(vix_df)} monthly observations")
    print(f"     Range: {vix_df['date'].min().date()} → "
          f"{vix_df['date'].max().date()}")
    print(f"     Missing: {vix_df['value'].isna().sum()}")
    print(f"     Min: {vix_df['value'].min():.2f} | "
          f"Max: {vix_df['value'].max():.2f} | "
          f"Latest: {vix_df['value'].iloc[-1]:.2f} "
          f"({vix_df['date'].iloc[-1].date()})")

except Exception as e:
    print(f"  ❌ VIX error: {e}")
    vix_df = None

# =============================================================================
# STEP 6: SAVE VIX TABLE
# =============================================================================
print("\nStep 6: Saving VIX table...")

if vix_df is not None:
    vix_warehouse = pd.DataFrame({
        "date":      vix_df["date"],
        "value":     vix_df["value"].round(2),
        "unit":      "Index (implied volatility %)",
        "source":    "Yahoo Finance — ^VIX",
        "frequency": "Monthly (average of daily closes)",
        "notes":     "CBOE Volatility Index — geopolitical and market "
                     "uncertainty proxy"
    })

    vix_warehouse.to_csv("data/warehouse/vix_yahoo.csv", index=False)
    print(f"  ✅ Saved: data/warehouse/vix_yahoo.csv")
    print(f"     {len(vix_warehouse)} rows × "
          f"{len(vix_warehouse.columns)} columns")
else:
    print(f"  ⚠️  VIX not saved — download failed")

# =============================================================================
# STEP 7: UPDATE DATA LINEAGE
# =============================================================================
print("\nStep 7: Updating data lineage...")

lineage_path = "data/processed/data_lineage.json"

try:
    with open(lineage_path, "r") as f:
        lineage = json.load(f)
except FileNotFoundError:
    lineage = {}

lineage["macro"] = {
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "fred_series": {
        sid: {
            "column": meta["col"],
            "unit":   meta["unit"],
            "notes":  meta["notes"]
        }
        for sid, meta in FRED_SERIES.items()
    },
    "vix": {
        "source":    "Yahoo Finance — ^VIX",
        "unit":      "Index (implied volatility %)",
        "frequency": "Monthly average of daily closes"
    },
    "date_range_start": START_DATE,
    "date_range_end":   str(macro_wide["date"].max().date()),
    "total_rows":       macro_wide.shape[0],
    "warehouse_files": {
        "fred": "data/warehouse/macro_fred.csv",
        "vix":  "data/warehouse/vix_yahoo.csv"
    }
}

with open(lineage_path, "w") as f:
    json.dump(lineage, f, indent=2)

print(f"  ✅ Data lineage saved: {lineage_path}")

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("WAREHOUSE FILES CREATED:")
print(f"  data/warehouse/macro_fred.csv")
print(f"  data/warehouse/vix_yahoo.csv")
print("=" * 60)
print(f"01d COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)