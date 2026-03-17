# =============================================================================
# 02: BUILD WAREHOUSE
# Validates all warehouse files, checks coverage and consistency,
# produces warehouse_summary.json as inventory for downstream scripts
# =============================================================================

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

os.makedirs("data/warehouse", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

print("=" * 60)
print("02: BUILD WAREHOUSE — VALIDATION & HEALTH CHECK")
print("=" * 60)

# =============================================================================
# WAREHOUSE FILE REGISTRY
# =============================================================================
WAREHOUSE_FILES = {
    "aluminum":  {
        "path":      "data/warehouse/aluminum_worldbank.csv",
        "price_col": "value",
        "label":     "Aluminum (World Bank)",
        "expected_min_rows": 300,
        "price_range": (500, 6000)
    },
    "lithium_combined": {
        "path":      "data/warehouse/lithium_combined.csv",
        "price_col": "value",
        "label":     "Lithium Combined (USGS+IMF)",
        "expected_min_rows": 300,
        "price_range": (10000, 600000)
    },
    "lithium_usgs": {
        "path":      "data/warehouse/lithium_usgs.csv",
        "price_col": "value",
        "label":     "Lithium USGS (reference)",
        "expected_min_rows": 20,
        "price_range": (1000, 10000)
    },
    "lithium_imf_raw": {
        "path":      "data/warehouse/lithium_imf_raw.csv",
        "price_col": "value",
        "label":     "Lithium IMF Raw (battery grade)",
        "expected_min_rows": 100,
        "price_range": (50000, 600000)
    },
    "cobalt_imf": {
        "path":      "data/warehouse/cobalt_imf_monthly.csv",
        "price_col": "value",
        "label":     "Cobalt IMF Monthly (primary)",
        "expected_min_rows": 300,
        "price_range": (5000, 200000)
    },
    "cobalt_usgs": {
        "path":      "data/warehouse/cobalt_usgs.csv",
        "price_col": "value",
        "label":     "Cobalt USGS (reference)",
        "expected_min_rows": 20,
        "price_range": (5000, 100000)
    },
    "macro_fred": {
        "path":      "data/warehouse/macro_fred.csv",
        "price_col": None,
        "label":     "Macro Variables (FRED)",
        "expected_min_rows": 300,
        "price_range": None
    },
    "vix": {
        "path":      "data/warehouse/vix_yahoo.csv",
        "price_col": "value",
        "label":     "VIX (Yahoo Finance)",
        "expected_min_rows": 300,
        "price_range": (10, 90)
    },
}

# =============================================================================
# STEP 1: LOAD AND VALIDATE ALL WAREHOUSE FILES
# =============================================================================
print("\nStep 1: Loading and validating warehouse files...")
print(f"\n  {'File':<30} {'Rows':>6} {'Start':>12} {'End':>12} "
      f"{'Missing':>8} {'Status':>8}")
print(f"  {'-'*30} {'-'*6} {'-'*12} {'-'*12} {'-'*8} {'-'*8}")

loaded = {}
issues = []

for key, meta in WAREHOUSE_FILES.items():
    path = meta["path"]

    if not os.path.exists(path):
        print(f"  {'❌ MISSING: ' + key:<30} {'—':>6} {'—':>12} "
              f"{'—':>12} {'—':>8} {'MISSING':>8}")
        issues.append(f"File missing: {path}")
        continue

    df = pd.read_csv(path, parse_dates=["date"])
    loaded[key] = df

    rows    = len(df)
    start   = df["date"].min().date()
    end     = df["date"].max().date()
    missing = df.isnull().sum().sum()
    status  = "✅ OK"

    # Check minimum rows
    if rows < meta["expected_min_rows"]:
        status = "⚠️ LOW"
        issues.append(f"{key}: only {rows} rows (expected ≥ "
                      f"{meta['expected_min_rows']})")

    # Check price range
    if meta["price_col"] and meta["price_range"]:
        col = meta["price_col"]
        if col in df.columns:
            min_p, max_p = meta["price_range"]
            actual_min = df[col].min()
            actual_max = df[col].max()
            if actual_min < min_p * 0.5 or actual_max > max_p * 2:
                status = "⚠️ RANGE"
                issues.append(f"{key}: price range "
                               f"${actual_min:,.0f}–${actual_max:,.0f} "
                               f"outside expected")

    print(f"  {meta['label']:<30} {rows:>6} {str(start):>12} "
          f"{str(end):>12} {missing:>8} {status:>8}")

# =============================================================================
# STEP 2: DATE COVERAGE ANALYSIS
# =============================================================================
print("\n\nStep 2: Date coverage analysis...")

reference_dates = pd.date_range(
    start="2000-01-01",
    end=pd.Timestamp.today().replace(day=1),
    freq="MS"
)

print(f"\n  Expected date range: 2000-01-01 → "
      f"{reference_dates[-1].date()} "
      f"({len(reference_dates)} months)")

coverage_files = [
    ("aluminum",        "Aluminum"),
    ("lithium_combined","Lithium Combined"),
    ("cobalt_imf",      "Cobalt IMF"),
    ("vix",             "VIX"),
]

for key, label in coverage_files:
    if key not in loaded:
        continue
    df = loaded[key]
    actual_dates = set(df["date"].dt.to_period("M"))
    expected_dates = set(reference_dates.to_period("M"))
    missing_months = expected_dates - actual_dates
    extra_months   = actual_dates - expected_dates

    coverage_pct = (len(actual_dates) / len(expected_dates)) * 100
    print(f"  {label:<25}: {coverage_pct:.1f}% coverage | "
          f"Missing months: {len(missing_months)} | "
          f"Extra months: {len(extra_months)}")

# =============================================================================
# STEP 3: CROSS-VALIDATE KEY PRICE BENCHMARKS
# =============================================================================
print("\n\nStep 3: Cross-validating price benchmarks...")

benchmarks = [
    ("aluminum",   "2008-09-01", 2572.0,  200, "2008 Financial Crisis"),
    ("aluminum",   "2022-03-01", 3494.0,  300, "2022 Russia-Ukraine spike"),
    ("cobalt_imf", "2018-03-01", 90000.0, 15000, "2018 Cobalt peak"),
    ("cobalt_imf", "2020-01-01", 30000.0, 10000, "2020 Cobalt trough"),
]

print(f"\n  {'Commodity':<12} {'Date':<12} {'Expected':>10} "
      f"{'Actual':>10} {'Diff %':>8} {'Event':<30}")
print(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*10} {'-'*8} {'-'*30}")

for key, date_str, expected, tolerance, event in benchmarks:
    if key not in loaded:
        continue
    df  = loaded[key]
    row = df[df["date"] == date_str]
    if len(row) == 0:
        print(f"  {key:<12} {date_str:<12} ${expected:>9,.0f} "
              f"{'N/A':>10} {'—':>8} {event}")
        continue
    actual   = row["value"].values[0]
    diff_pct = ((actual - expected) / expected) * 100
    ok       = "✅" if abs(actual - expected) < tolerance * 2 else "⚠️"
    print(f"  {key:<12} {date_str:<12} ${expected:>9,.0f} "
          f"${actual:>9,.0f} {diff_pct:>+7.1f}% {ok} {event}")

# =============================================================================
# STEP 4: MACRO VARIABLE SPOT CHECK
# =============================================================================
print("\n\nStep 4: Macro variable spot check...")

if "macro_fred" in loaded:
    macro = loaded["macro_fred"]
    macro_cols = ["wti_crude_usd", "natural_gas_usd",
                  "industrial_production_idx", "usd_cny"]

    print(f"\n  {'Series':<30} {'Min':>10} {'Max':>10} "
          f"{'Latest':>10} {'Latest Date':>12}")
    print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

    for col in macro_cols:
        if col not in macro.columns:
            continue
        series     = macro[col].dropna()
        latest_val = series.iloc[-1]
        latest_dt  = macro.loc[macro[col].notna(), "date"].iloc[-1].date()
        print(f"  {col:<30} {series.min():>10.2f} {series.max():>10.2f} "
              f"{latest_val:>10.2f} {str(latest_dt):>12}")

# =============================================================================
# STEP 5: ISSUES SUMMARY
# =============================================================================
print("\n\nStep 5: Issues summary...")

if issues:
    print(f"  ⚠️  {len(issues)} issue(s) found:")
    for issue in issues:
        print(f"     — {issue}")
else:
    print(f"  ✅ No issues found — all warehouse files healthy")

# =============================================================================
# STEP 6: SAVE WAREHOUSE SUMMARY
# =============================================================================
print("\nStep 6: Saving warehouse summary...")

summary = {
    "last_validated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "status":         "HEALTHY" if not issues else "WARNINGS",
    "issues":         issues,
    "files": {}
}

for key, meta in WAREHOUSE_FILES.items():
    if key not in loaded:
        summary["files"][key] = {"status": "MISSING"}
        continue
    df = loaded[key]
    summary["files"][key] = {
        "status":     "OK",
        "path":       meta["path"],
        "rows":       len(df),
        "date_start": str(df["date"].min().date()),
        "date_end":   str(df["date"].max().date()),
        "missing":    int(df.isnull().sum().sum())
    }

summary_path = "data/processed/warehouse_summary.json"
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2)

print(f"  ✅ Saved: {summary_path}")

print("\n" + "=" * 60)
print(f"  Warehouse status: {summary['status']}")
print(f"  Files validated : {len(loaded)}/{len(WAREHOUSE_FILES)}")
print(f"  Issues found    : {len(issues)}")
print("=" * 60)
print(f"02 COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)
