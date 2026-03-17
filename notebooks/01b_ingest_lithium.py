# =============================================================================
# 01b: LITHIUM INGESTION
# Sources:
#   - USGS Mineral Commodity Summaries (annual, pre-2012)
#   - IMF PCPS Battery Grade 99% (monthly, post-2012)
# Method:
#   Option C — USGS pre-2012 scaled UP to IMF battery grade equivalent
#   IMF series is authoritative — USGS normalized to match IMF scale
#   Pre-2012: Annual USGS linearly interpolated to monthly
#             Lithium was low-volatility industrial commodity in this period
#   Post-2012: IMF monthly actual prices used as-is
# Outputs:
#   data/warehouse/lithium_usgs.csv           (USGS annual, original scale)
#   data/warehouse/lithium_imf_raw.csv        (IMF monthly, battery grade)
#   data/warehouse/lithium_combined.csv       (final spliced series)
# Schema: date | value | unit | source | frequency | notes
# =============================================================================

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# --- Dependency check ---------------------------------------------------------
USGS_PATH = "data/raw/usgs_lithium.xlsx"
IMF_PATH  = "data/raw/imf_commodity_prices.csv"
os.makedirs("data/warehouse", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

for path in [USGS_PATH, IMF_PATH]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file not found: {path}")

print("=" * 60)
print("01b: LITHIUM INGESTION")
print("=" * 60)

# =============================================================================
# STEP 1: LOAD USGS ANNUAL LITHIUM CARBONATE PRICES
# =============================================================================
print("\nStep 1: Loading USGS annual lithium carbonate prices...")

usgs_raw = pd.read_excel(USGS_PATH, skiprows=4, engine="openpyxl")

# Find price column — unit value, exclude real (98$) column
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
print(f"\n  Raw USGS values (carbonate $/mt):")
print(usgs.to_string(index=False))

# Save USGS warehouse table in original scale for reference
usgs_warehouse_raw = pd.DataFrame({
    "date":      pd.to_datetime(usgs["year"].astype(str) + "-01-01"),
    "value":     usgs["value"].round(2),
    "unit":      "USD per metric ton (lithium carbonate)",
    "source":    "USGS Mineral Commodity Summaries",
    "frequency": "Annual",
    "notes":     "Original USGS carbonate prices — not normalized"
})
usgs_warehouse_raw.to_csv("data/warehouse/lithium_usgs.csv", index=False)
print(f"\n  ✅ Saved original scale: data/warehouse/lithium_usgs.csv")

# =============================================================================
# STEP 2: LOAD IMF MONTHLY LITHIUM PRICES (BATTERY GRADE 99%)
# =============================================================================
print("\nStep 2: Loading IMF monthly lithium prices (battery grade)...")

imf_raw = pd.read_csv(IMF_PATH)

imf_lithium_row = imf_raw[
    (imf_raw["INDICATOR"].str.contains("Lithium", na=False)) &
    (imf_raw["OBS_MEASURE"] == "OBS_VALUE") &
    (imf_raw["DATA_TRANSFORMATION"] == "US dollars") &
    (imf_raw["FREQUENCY"] == "Monthly")
]

print(f"  IMF lithium rows found: {len(imf_lithium_row)}")

monthly_cols = [c for c in imf_raw.columns if "-M" in str(c)]

imf_long = imf_lithium_row[monthly_cols].T.reset_index()
imf_long.columns = ["period", "value"]
imf_long = imf_long[imf_long["value"] != ""]
imf_long["value"] = pd.to_numeric(imf_long["value"], errors="coerce")
imf_long["date"]  = pd.to_datetime(
    imf_long["period"].str.replace("-M", "-", regex=False),
    format="%Y-%m", errors="coerce"
)
imf_long = imf_long.dropna(subset=["date", "value"])
imf_long = imf_long[["date", "value"]].sort_values("date").reset_index(drop=True)
imf_long = imf_long[imf_long["date"] >= "2012-06-01"]

print(f"  ✅ IMF monthly: {len(imf_long)} observations")
print(f"     Range: {imf_long['date'].min().date()} → "
      f"{imf_long['date'].max().date()}")
print(f"     Unit: USD per metric ton (99% battery grade lithium)")

# Save IMF raw warehouse table
imf_warehouse_raw = pd.DataFrame({
    "date":      imf_long["date"],
    "value":     imf_long["value"].round(2),
    "unit":      "USD per metric ton (99% battery grade)",
    "source":    "IMF Primary Commodity Price System (PCPS)",
    "frequency": "Monthly",
    "notes":     "Raw IMF battery grade prices — authoritative series"
})
imf_warehouse_raw.to_csv("data/warehouse/lithium_imf_raw.csv", index=False)
print(f"\n  ✅ Saved IMF raw: data/warehouse/lithium_imf_raw.csv")

# =============================================================================
# STEP 3: CALCULATE BRIDGING FACTOR AT 2012 OVERLAP
# IMF is authoritative — USGS scaled UP to match IMF battery grade scale
# bridging_factor = IMF_2012_avg / USGS_2012_annual
# =============================================================================
print("\nStep 3: Calculating bridging factor at 2012 overlap...")

# USGS 2012 annual value
usgs_2012_annual = usgs[usgs["year"] == 2012]["value"].values[0]

# IMF 2012 annual average (from monthly data)
imf_2012_avg = imf_long[
    imf_long["date"].dt.year == 2012
]["value"].mean()

# Bridging factor: scale USGS up to IMF level
bridging_factor = imf_2012_avg / usgs_2012_annual

print(f"  USGS 2012 annual price   : ${usgs_2012_annual:>12,.2f}/mt (carbonate)")
print(f"  IMF  2012 annual average : ${imf_2012_avg:>12,.2f}/mt (battery grade)")
print(f"  Bridging factor          : {bridging_factor:.6f}")
print(f"  Interpretation: USGS prices × {bridging_factor:.4f} = battery grade equivalent")

# =============================================================================
# STEP 4: SCALE USGS PRE-2012 TO BATTERY GRADE EQUIVALENT
# Apply bridging factor to entire USGS series
# Then interpolate to monthly
# =============================================================================
print("\nStep 4: Scaling USGS to battery grade equivalent + monthly interpolation...")

# Scale USGS annual values
usgs_scaled = usgs.copy()
usgs_scaled["value"] = (usgs_scaled["value"] * bridging_factor).round(2)

print(f"  Scaled USGS values (battery grade equivalent $/mt):")
print(usgs_scaled.to_string(index=False))

# Interpolate scaled USGS to monthly — only pre-2012
monthly_dates_usgs = pd.date_range(
    start=f"{usgs_scaled['year'].min()}-01-01",
    end="2012-05-01",  # IMF starts June 2012
    freq="MS"
)

usgs_scaled["date"] = pd.to_datetime(
    usgs_scaled["year"].astype(str) + "-01-01"
)

usgs_monthly_scaled = (
    usgs_scaled.set_index("date")["value"]
    .reindex(monthly_dates_usgs)
    .interpolate(method="linear")
    .reset_index()
)
usgs_monthly_scaled.columns = ["date", "value"]

print(f"\n  ✅ USGS monthly interpolated (pre-2012): "
      f"{len(usgs_monthly_scaled)} rows")
print(f"     Range: {usgs_monthly_scaled['date'].min().date()} → "
      f"{usgs_monthly_scaled['date'].max().date()}")

# =============================================================================
# STEP 5: VALIDATE BRIDGING — CHECK 2012 CONTINUITY
# =============================================================================
print("\nStep 5: Validating splice continuity at 2012...")

# Last USGS scaled value (May 2012 — last interpolated month)
may_2012 = usgs_monthly_scaled[
    usgs_monthly_scaled["date"] == "2012-05-01"
]["value"].values[0]

# First IMF value (Jun 2012 — first actual month)
jun_2012 = imf_long[
    imf_long["date"] == "2012-06-01"
]["value"].values[0]

# USGS 2012 scaled annual for comparison
usgs_2012_scaled = usgs_scaled[
    usgs_scaled["year"] == 2012
]["value"].values[0]

discontinuity     = abs(jun_2012 - may_2012)
discontinuity_pct = (discontinuity / may_2012) * 100

print(f"  May 2012 — last USGS scaled  : ${may_2012:>10,.2f}/mt")
print(f"  Jun 2012 — first IMF actual  : ${jun_2012:>10,.2f}/mt")
print(f"  USGS 2012 scaled annual      : ${usgs_2012_scaled:>10,.2f}/mt")
print(f"  IMF  2012 annual average     : ${imf_2012_avg:>10,.2f}/mt")
print(f"  Splice discontinuity         : ${discontinuity:>10,.2f}/mt "
      f"({discontinuity_pct:.1f}%)")

if discontinuity_pct < 5:
    print(f"  ✅ Smooth splice — discontinuity < 5%")
elif discontinuity_pct < 15:
    print(f"  ⚠️  Minor discontinuity — acceptable, documented in lineage")
else:
    print(f"  ❌ Large discontinuity — review bridging approach")

# =============================================================================
# STEP 6: SPLICE INTO ONE CONTINUOUS COMBINED SERIES
# pre-2012  → USGS scaled to battery grade, monthly interpolated
# 2012+     → IMF monthly actual, battery grade (as-is)
# =============================================================================
print("\nStep 6: Splicing into combined series...")

# Build pre-2012 warehouse slice
pre_2012 = pd.DataFrame({
    "date":      usgs_monthly_scaled["date"],
    "value":     usgs_monthly_scaled["value"],
    "unit":      "USD per metric ton (battery grade equivalent)",
    "source":    "USGS Mineral Commodity Summaries (scaled)",
    "frequency": "Monthly (interpolated from annual)",
    "notes":     (f"USGS carbonate prices scaled to IMF battery grade. "
                  f"Bridging factor: {bridging_factor:.6f}. "
                  f"Annual data linearly interpolated to monthly.")
})

# Build post-2012 warehouse slice
post_2012 = pd.DataFrame({
    "date":      imf_long["date"],
    "value":     imf_long["value"].round(2),
    "unit":      "USD per metric ton (99% battery grade)",
    "source":    "IMF Primary Commodity Price System (PCPS)",
    "frequency": "Monthly",
    "notes":     "IMF actual monthly prices — authoritative series"
})

combined = pd.concat([pre_2012, post_2012], ignore_index=True)
combined = combined.sort_values("date").reset_index(drop=True)

print(f"  Pre-2012  (USGS scaled)  : {len(pre_2012)} rows")
print(f"  Post-2012 (IMF actual)   : {len(post_2012)} rows")
print(f"  Combined total           : {len(combined)} rows")
print(f"  Range: {combined['date'].min().date()} → "
      f"{combined['date'].max().date()}")

# =============================================================================
# STEP 7: FINAL VALIDATION
# =============================================================================
print("\nStep 7: Final validation...")

missing = combined["value"].isna().sum()
print(f"  Missing values : {missing}")
print(f"  Min price      : ${combined['value'].min():>10,.2f}/mt")
print(f"  Max price      : ${combined['value'].max():>10,.2f}/mt")
print(f"  Latest price   : ${combined['value'].iloc[-1]:>10,.2f}/mt "
      f"({combined['date'].iloc[-1].date()})")

# Print splice window for visual inspection
print(f"\n  Splice window — 6 months around June 2012:")
splice = combined[
    (combined["date"] >= "2012-01-01") &
    (combined["date"] <= "2012-12-01")
][["date", "value", "source"]]
print(splice.to_string(index=False))

if missing > 0:
    print(f"\n  ⚠️  {missing} missing values — review pipeline")
else:
    print(f"\n  ✅ No missing values")

# =============================================================================
# STEP 8: SAVE COMBINED SERIES
# =============================================================================
print("\nStep 8: Saving combined series...")

combined.to_csv("data/warehouse/lithium_combined.csv", index=False)
print(f"  ✅ Saved: data/warehouse/lithium_combined.csv")

# =============================================================================
# STEP 9: UPDATE DATA LINEAGE
# =============================================================================
print("\nStep 9: Updating data lineage...")

lineage_path = "data/processed/data_lineage.json"

try:
    with open(lineage_path, "r") as f:
        lineage = json.load(f)
except FileNotFoundError:
    lineage = {}

lineage["lithium"] = {
    "last_updated":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "unit":                   "USD per metric ton (battery grade equivalent)",
    "pre_2012": {
        "source":             "USGS Mineral Commodity Summaries",
        "original_unit":      "USD per metric ton (lithium carbonate)",
        "method":             "Annual prices scaled to IMF battery grade, "
                              "then linearly interpolated to monthly",
        "note":               "Lithium was low-volatility industrial commodity "
                              "pre-2012. Annual interpolation is appropriate.",
        "bridging_factor":    round(float(bridging_factor), 6),
        "usgs_2012_original": round(float(usgs_2012_annual), 2),
        "imf_2012_average":   round(float(imf_2012_avg), 2)
    },
    "post_2012": {
        "source":             "IMF Primary Commodity Price System (PCPS)",
        "unit":               "USD per metric ton (99% battery grade)",
        "method":             "Monthly actual prices — no transformation",
        "frequency":          "Monthly"
    },
    "splice_date":            "2012-06-01",
    "splice_discontinuity_pct": round(float(discontinuity_pct), 2),
    "total_rows":             len(combined),
    "date_range_start":       str(combined["date"].min().date()),
    "date_range_end":         str(combined["date"].max().date()),
    "warehouse_files": {
        "usgs_original":      "data/warehouse/lithium_usgs.csv",
        "imf_raw":            "data/warehouse/lithium_imf_raw.csv",
        "combined":           "data/warehouse/lithium_combined.csv"
    }
}

with open(lineage_path, "w") as f:
    json.dump(lineage, f, indent=2)

print(f"  ✅ Data lineage saved: {lineage_path}")

print("\n" + "=" * 60)
print(f"01b COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)