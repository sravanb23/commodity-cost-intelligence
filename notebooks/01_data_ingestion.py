# =============================================================================
# BLOCK 1: DATA INGESTION PIPELINE
# Electronics Commodity Cost Intelligence Platform
# Aluminum:     World Bank Pink Sheet (Monthly)
# Lithium:      USGS Mineral Commodity Summaries (Annual → Interpolated)
# Cobalt:       USGS Mineral Commodity Summaries (Annual → Interpolated)
# Macro:        FRED API — WTI Crude, Natural Gas, Industrial Production, FX
# Uncertainty:  Yahoo Finance — VIX
# =============================================================================

import pandas as pd
import numpy as np
import yfinance as yf
from fredapi import Fred

FRED_API_KEY = "ac39f6d4a1fe61821bfb838c11abe140"

# =============================================================================
# STEP 1: ALUMINUM — World Bank Pink Sheet
# =============================================================================

print("=" * 60)
print("STEP 1: Downloading Aluminum from World Bank Pink Sheet...")
print("=" * 60)

WORLD_BANK_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "18675f1d1639c7a34d463f59263ba0a2-0050012025"
    "/related/CMO-Historical-Data-Monthly.xlsx"
)

try:
    wb_raw = pd.read_excel(
        WORLD_BANK_URL,
        sheet_name="Monthly Prices",
        skiprows=4,
        engine="openpyxl"
    )

    date_col = wb_raw.columns[0]
    alum_col = [c for c in wb_raw.columns if "Aluminum" in str(c)][0]

    aluminum_df = wb_raw[[date_col, alum_col]].copy()
    aluminum_df.columns = ["date", "aluminum_usd_mt"]
    aluminum_df = aluminum_df.iloc[1:].copy()
    aluminum_df = aluminum_df.dropna(subset=["date"])
    aluminum_df["date"] = aluminum_df["date"].astype(str).str.strip()
    aluminum_df["date"] = aluminum_df["date"].str.replace("M", "-", regex=False)
    aluminum_df["date"] = pd.to_datetime(
        aluminum_df["date"], format="%Y-%m", errors="coerce"
    )
    aluminum_df = aluminum_df.dropna(subset=["date"])
    aluminum_df["aluminum_usd_mt"] = pd.to_numeric(
        aluminum_df["aluminum_usd_mt"], errors="coerce"
    )
    aluminum_df = aluminum_df[
        aluminum_df["date"] >= "2000-01-01"
    ].reset_index(drop=True)

    aluminum_df.to_csv("data/raw/aluminum_raw.csv", index=False)
    print(f"✅ Aluminum: {len(aluminum_df)} rows")
    print(f"   Range: {aluminum_df['date'].min()} → {aluminum_df['date'].max()}")

except Exception as e:
    print(f"❌ Aluminum error: {e}")
    aluminum_df = None

# =============================================================================
# STEP 2: LITHIUM & COBALT — USGS Mineral Commodity Summaries
# Column: "Unit value ($/t)" — annual spot price per metric ton
# =============================================================================

print("\n" + "=" * 60)
print("STEP 2: Loading USGS Lithium & Cobalt annual prices...")
print("=" * 60)

def load_usgs_commodity(filepath, commodity_name):
    """
    Loads USGS historical statistics Excel file,
    extracts Year and Unit value ($/t) columns,
    filters to 2000 onwards, returns clean annual dataframe.
    """
    try:
        df = pd.read_excel(
            filepath,
            skiprows=4,
            engine="openpyxl"
        )

        # Find the nominal unit value column (exclude 98$ real price column)
        price_col = [
            c for c in df.columns
            if "unit value" in str(c).lower()
            and "98$" not in str(c).lower()
        ][0]

        print(f"  ✅ {commodity_name} price column: '{price_col}'")

        result = df[["Year", price_col]].copy()
        result.columns = ["year", f"{commodity_name}_usd_mt"]
        result["year"] = pd.to_numeric(result["year"], errors="coerce")
        result[f"{commodity_name}_usd_mt"] = pd.to_numeric(
            result[f"{commodity_name}_usd_mt"], errors="coerce"
        )
        result = result.dropna(subset=["year", f"{commodity_name}_usd_mt"])
        result["year"] = result["year"].astype(int)
        result = result[result["year"] >= 2000].reset_index(drop=True)

        print(f"  ✅ {commodity_name}: {len(result)} annual observations")
        print(f"     Range: {result['year'].min()} → {result['year'].max()}")
        print(result.to_string())

        return result

    except Exception as e:
        print(f"  ❌ {commodity_name} error: {e}")
        return None

lithium_annual_df = load_usgs_commodity("data/raw/usgs_lithium.xlsx", "lithium")
cobalt_annual_df  = load_usgs_commodity("data/raw/usgs_cobalt.xlsx",  "cobalt")

# =============================================================================
# STEP 3: INTERPOLATE ANNUAL USGS DATA TO MONTHLY FREQUENCY
# Method: Linear interpolation between annual data points
# =============================================================================

print("\n" + "=" * 60)
print("STEP 3: Interpolating annual USGS data to monthly...")
print("=" * 60)

def interpolate_annual_to_monthly(annual_df, value_col):
    """
    Converts annual price data to monthly frequency using
    linear interpolation between annual anchor points.
    """
    monthly_dates = pd.date_range(
        start=f"{annual_df['year'].min()}-01-01",
        end=f"{annual_df['year'].max()}-12-01",
        freq="MS"
    )

    annual_df = annual_df.copy()
    annual_df["date"] = pd.to_datetime(
        annual_df["year"].astype(str) + "-01-01"
    )

    monthly = (
        annual_df.set_index("date")[value_col]
        .reindex(monthly_dates)
        .interpolate(method="linear")
        .reset_index()
    )
    monthly.columns = ["date", value_col]

    print(f"✅ {value_col}: {len(monthly)} monthly rows interpolated")
    return monthly

lithium_monthly = None
cobalt_monthly  = None

if lithium_annual_df is not None:
    lithium_monthly = interpolate_annual_to_monthly(
        lithium_annual_df, "lithium_usd_mt"
    )
    lithium_monthly.to_csv("data/raw/lithium_monthly.csv", index=False)

if cobalt_annual_df is not None:
    cobalt_monthly = interpolate_annual_to_monthly(
        cobalt_annual_df, "cobalt_usd_mt"
    )
    cobalt_monthly.to_csv("data/raw/cobalt_monthly.csv", index=False)

# =============================================================================
# STEP 4: FX USD/CNY — FRED API
# =============================================================================

print("\n" + "=" * 60)
print("STEP 4: Pulling USD/CNY FX rate from FRED...")
print("=" * 60)

try:
    fred = Fred(api_key=FRED_API_KEY)
    fx_raw = fred.get_series(
        "DEXCHUS",
        observation_start="2000-01-01",
        observation_end="2025-12-31"
    )

    fx_df = fx_raw.reset_index()
    fx_df.columns = ["date", "usd_cny"]
    fx_df["date"] = pd.to_datetime(fx_df["date"])
    fx_df = (
        fx_df.set_index("date")
             .resample("MS")
             .mean()
             .reset_index()
    )
    fx_df["usd_cny"] = pd.to_numeric(fx_df["usd_cny"], errors="coerce")
    fx_df.to_csv("data/raw/fx_usd_cny_raw.csv", index=False)
    print(f"✅ FX USD/CNY: {len(fx_df)} rows")
    print(f"   Range: {fx_df['date'].min()} → {fx_df['date'].max()}")

except Exception as e:
    print(f"❌ FRED FX error: {e}")
    fx_df = None

# =============================================================================
# STEP 5: MACRO VARIABLES — FRED API
# WTI Crude Oil, Natural Gas, Industrial Production Index
# =============================================================================

print("\n" + "=" * 60)
print("STEP 5: Pulling macro variables from FRED...")
print("=" * 60)

macro_series = {
    "DCOILWTICO": "wti_crude_usd",
    "MHHNGSP":    "natural_gas_usd",
    "IPMAN":      "industrial_production_idx"
}

macro_dfs = []

for series_id, col_name in macro_series.items():
    try:
        raw = fred.get_series(
            series_id,
            observation_start="2000-01-01",
            observation_end="2025-12-31"
        )
        df = raw.reset_index()
        df.columns = ["date", col_name]
        df["date"] = pd.to_datetime(df["date"])
        df = (
            df.set_index("date")
              .resample("MS")
              .mean()
              .reset_index()
        )
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        macro_dfs.append(df)
        print(f"✅ {series_id} ({col_name}): {len(df)} rows")

    except Exception as e:
        print(f"❌ {series_id} error: {e}")

# =============================================================================
# STEP 6: VIX — Yahoo Finance
# Market uncertainty / geopolitical risk proxy
# =============================================================================

print("\n" + "=" * 60)
print("STEP 6: Pulling VIX from Yahoo Finance...")
print("=" * 60)

try:
    vix_raw = yf.download(
        "^VIX",
        start="2000-01-01",
        end="2025-12-31",
        progress=False
    )

    vix_df = (
        vix_raw[["Close"]]
        .resample("MS")
        .mean()
        .reset_index()
    )
    vix_df.columns = ["date", "vix"]
    vix_df["date"] = pd.to_datetime(vix_df["date"])
    vix_df["vix"] = pd.to_numeric(vix_df["vix"], errors="coerce")
    print(f"✅ VIX: {len(vix_df)} rows")

except Exception as e:
    print(f"❌ VIX error: {e}")
    vix_df = None

# =============================================================================
# STEP 7: MERGE ALL SOURCES INTO MASTER DATASET
# =============================================================================

print("\n" + "=" * 60)
print("STEP 7: Merging all sources into master dataset...")
print("=" * 60)

try:
    master = aluminum_df.copy()

    # Merge USGS commodities
    if lithium_monthly is not None:
        master = master.merge(lithium_monthly, on="date", how="left")
    if cobalt_monthly is not None:
        master = master.merge(cobalt_monthly, on="date", how="left")

    # Merge macro FRED series
    for df in macro_dfs:
        master = master.merge(df, on="date", how="left")

    # Merge VIX
    if vix_df is not None:
        master = master.merge(vix_df, on="date", how="left")

    # Merge FX
    if fx_df is not None:
        master = master.merge(fx_df, on="date", how="left")

    # Add time columns
    master = master.sort_values("date").reset_index(drop=True)
    master["year"]    = master["date"].dt.year
    master["month"]   = master["date"].dt.month
    master["quarter"] = master["date"].dt.quarter

    print(f"✅ Master dataset: {master.shape[0]} rows, "
          f"{master.shape[1]} columns")
    print(f"   Range: {master['date'].min()} → {master['date'].max()}")
    print(f"\nColumns: {list(master.columns)}")
    print(f"\nMissing values:\n{master.isnull().sum()}")
    print(f"\nPreview:\n{master.head().to_string()}")

    master.to_csv("data/processed/commodity_master.csv", index=False)
    print(f"\n✅ Saved to data/processed/commodity_master.csv")

except Exception as e:
    print(f"❌ Merge error: {e}")

print("\n" + "=" * 60)
print("BLOCK 1 COMPLETE ✅")
print("=" * 60)