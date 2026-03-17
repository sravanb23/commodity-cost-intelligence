# =============================================================================
# 04: EDA + STRUCTURAL BREAK DETECTION
# Reads from: data/processed/commodity_master.csv
# Outputs:    data/outputs/ (charts + tables)
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import os
from datetime import datetime
warnings.filterwarnings("ignore")

os.makedirs("data/outputs", exist_ok=True)

print("=" * 60)
print("04: EDA + STRUCTURAL BREAK DETECTION")
print("=" * 60)

# =============================================================================
# LOAD MASTER DATASET
# =============================================================================
MASTER_PATH = "data/processed/commodity_master.csv"
if not os.path.exists(MASTER_PATH):
    raise FileNotFoundError(
        f"{MASTER_PATH} not found. Run run_ingestion.py first."
    )

master = pd.read_csv(MASTER_PATH, parse_dates=["date"])
print(f"\n✅ Loaded: {master.shape[0]} rows × {master.shape[1]} columns")
print(f"   Range: {master['date'].min().date()} → "
      f"{master['date'].max().date()}")

# Filter to where all three commodities have data
master_full = master.dropna(
    subset=["aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt"]
).copy().reset_index(drop=True)
print(f"   Analysis dataset (all 3 commodities): "
      f"{master_full.shape[0]} rows")

# =============================================================================
# KEY EVENTS FOR ANNOTATIONS
# =============================================================================
EVENTS = {
    "2003-03": "Iraq War",
    "2008-09": "Financial\nCrisis",
    "2011-01": "Arab\nSpring",
    "2020-03": "COVID-19",
    "2021-06": "EV/Battery\nBoom",
    "2022-02": "Russia-\nUkraine",
    "2022-10": "China\nLockdowns",
}

COMMODITIES = [
    ("aluminum_usd_mt", "Aluminum ($/mt)",  "#2196F3"),
    ("lithium_usd_mt",  "Lithium ($/mt)",   "#4CAF50"),
    ("cobalt_usd_mt",   "Cobalt ($/mt)",    "#FF5722"),
]

VOL_COLS = [
    ("aluminum_usd_mt", "Aluminum", "#2196F3"),
    ("lithium_usd_mt",  "Lithium",  "#4CAF50"),
    ("cobalt_usd_mt",   "Cobalt",   "#FF5722"),
]

# =============================================================================
# SECTION 1: COMMODITY PRICE TRENDS
# =============================================================================
print("\n" + "=" * 60)
print("Section 1: Commodity Price Trends...")
print("=" * 60)

fig, axes = plt.subplots(3, 1, figsize=(16, 14))
fig.suptitle(
    "Electronics Commodity Price Trends\n"
    "Aluminum | Lithium (Battery Grade) | Cobalt",
    fontsize=14, fontweight="bold", y=0.98
)

for ax, (col, label, color) in zip(axes, COMMODITIES):
    ax.plot(
        master_full["date"], master_full[col],
        color=color, linewidth=1.8, label=label
    )
    rolling = master_full[col].rolling(12).mean()
    ax.plot(
        master_full["date"], rolling,
        color="black", linewidth=1.2,
        linestyle="--", alpha=0.6, label="12M Rolling Avg"
    )
    for event_date, event_label in EVENTS.items():
        ed = pd.to_datetime(event_date)
        if master_full["date"].min() <= ed <= master_full["date"].max():
            ax.axvline(x=ed, color="red", linestyle=":", alpha=0.5, linewidth=1)
            ymax = master_full[col].max()
            ax.text(ed, ymax * 0.95, event_label,
                    fontsize=7, color="red", rotation=90,
                    va="top", ha="right")
    ax.set_ylabel(label, fontsize=10)
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(True, alpha=0.3)
    ax.set_xlim(master_full["date"].min(), master_full["date"].max())

axes[-1].set_xlabel("Date", fontsize=10)
plt.tight_layout()
plt.savefig("data/outputs/01_commodity_price_trends.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 01_commodity_price_trends.png")

# =============================================================================
# SECTION 2: YEAR-OVER-YEAR VARIANCE ANALYSIS
# =============================================================================
print("\nSection 2: Year-over-Year Variance Analysis...")

annual = master_full.groupby("year")[[
    "aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt"
]].mean().reset_index()

for col in ["aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt"]:
    annual[f"{col}_yoy"] = annual[col].pct_change() * 100

print("\nYear-over-Year Price Changes (%):")
yoy_display = annual[[
    "year", "aluminum_usd_mt_yoy",
    "lithium_usd_mt_yoy", "cobalt_usd_mt_yoy"
]].round(1)
print(yoy_display.to_string(index=False))

annual.to_csv("data/outputs/annual_price_variance.csv", index=False)

fig, axes = plt.subplots(3, 1, figsize=(16, 12))
fig.suptitle(
    "Year-over-Year Price Variance (%) — PPV Analysis\n"
    "Aluminum | Lithium | Cobalt",
    fontsize=14, fontweight="bold"
)

yoy_cols = [
    ("aluminum_usd_mt_yoy", "Aluminum YoY %", "#2196F3"),
    ("lithium_usd_mt_yoy",  "Lithium YoY %",  "#4CAF50"),
    ("cobalt_usd_mt_yoy",   "Cobalt YoY %",   "#FF5722"),
]

for ax, (col, label, color) in zip(axes, yoy_cols):
    yoy_data = annual.dropna(subset=[col])
    bars = ax.bar(
        yoy_data["year"], yoy_data[col],
        color=[color if v >= 0 else "#EF5350" for v in yoy_data[col]],
        alpha=0.8, edgecolor="white"
    )
    ax.axhline(y=0, color="black", linewidth=0.8)
    ax.set_ylabel(label, fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")
    for _, row in yoy_data.iterrows():
        if abs(row[col]) > 20:
            ax.text(
                row["year"],
                row[col] + (2 if row[col] > 0 else -6),
                f"{row[col]:.0f}%",
                ha="center", fontsize=7, fontweight="bold"
            )

axes[-1].set_xlabel("Year", fontsize=10)
plt.tight_layout()
plt.savefig("data/outputs/02_yoy_variance_analysis.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 02_yoy_variance_analysis.png")

# =============================================================================
# SECTION 3: VOLATILITY ANALYSIS
# =============================================================================
print("\nSection 3: Volatility Analysis...")

fig, ax = plt.subplots(figsize=(16, 6))
fig.suptitle(
    "Commodity Price Volatility — Rolling 12-Month Coefficient of Variation",
    fontsize=14, fontweight="bold"
)

for col, label, color in VOL_COLS:
    rolling_std  = master_full[col].rolling(12).std()
    rolling_mean = master_full[col].rolling(12).mean()
    norm_vol = (rolling_std / rolling_mean) * 100
    ax.plot(master_full["date"], norm_vol,
            label=f"{label} Volatility (%)",
            color=color, linewidth=1.8)

for event_date in EVENTS:
    ed = pd.to_datetime(event_date)
    if master_full["date"].min() <= ed <= master_full["date"].max():
        ax.axvline(x=ed, color="red", linestyle=":", alpha=0.4, linewidth=1)

ax.set_ylabel("Coefficient of Variation (%)", fontsize=10)
ax.set_xlabel("Date", fontsize=10)
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("data/outputs/03_volatility_analysis.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 03_volatility_analysis.png")

# =============================================================================
# SECTION 4: STRUCTURAL BREAK DETECTION (CUSUM)
# =============================================================================
print("\nSection 4: CUSUM Structural Break Detection...")

def detect_cusum_breaks(series):
    s      = series.dropna().values
    mean_s = np.mean(s)
    cusum  = np.cumsum(s - mean_s)
    threshold = np.std(cusum) * 1.5
    breaks = []
    for i in range(1, len(cusum) - 1):
        if abs(cusum[i]) > threshold:
            if ((cusum[i] > cusum[i-1] and cusum[i] > cusum[i+1]) or
                    (cusum[i] < cusum[i-1] and cusum[i] < cusum[i+1])):
                breaks.append(i)
    return cusum, breaks

fig, axes = plt.subplots(3, 1, figsize=(16, 12))
fig.suptitle(
    "Structural Break Detection — CUSUM Analysis\n"
    "Identifying Price Regime Changes",
    fontsize=14, fontweight="bold"
)

for ax, (col, label, color) in zip(axes, VOL_COLS):
    series = master_full[col]
    cusum, breaks = detect_cusum_breaks(series)
    valid_dates   = master_full.loc[series.notna(), "date"].values

    ax.plot(valid_dates, cusum, color=color, linewidth=1.8,
            label=f"{label} CUSUM")
    ax.axhline(y=0, color="black", linewidth=0.8, linestyle="--")

    for b in breaks[:5]:
        if b < len(valid_dates):
            ax.axvline(x=valid_dates[b], color="red",
                       linestyle="--", alpha=0.7, linewidth=1.2)

    ax.fill_between(valid_dates, cusum, 0,
                    where=[c > 0 for c in cusum],
                    alpha=0.15, color="green", label="Above-mean regime")
    ax.fill_between(valid_dates, cusum, 0,
                    where=[c < 0 for c in cusum],
                    alpha=0.15, color="red", label="Below-mean regime")

    ax.set_ylabel(f"{label} CUSUM", fontsize=10)
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(True, alpha=0.3)

axes[-1].set_xlabel("Date", fontsize=10)
plt.tight_layout()
plt.savefig("data/outputs/04_structural_breaks_cusum.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 04_structural_breaks_cusum.png")

# =============================================================================
# SECTION 5: MACRO CORRELATION MATRIX
# =============================================================================
print("\nSection 5: Macro Correlation Matrix...")

corr_cols = [
    "aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt",
    "wti_crude_usd", "natural_gas_usd",
    "industrial_production_idx", "vix", "usd_cny"
]
corr_labels = [
    "Aluminum", "Lithium", "Cobalt",
    "WTI Crude", "Natural Gas",
    "Industrial Prod", "VIX", "USD/CNY"
]

corr_matrix = master_full[corr_cols].corr()

fig, ax = plt.subplots(figsize=(12, 10))
fig.suptitle(
    "Macro-Commodity Correlation Matrix\nIdentifying Key Price Drivers",
    fontsize=14, fontweight="bold"
)

im = ax.imshow(corr_matrix.values, cmap="RdYlGn",
               vmin=-1, vmax=1, aspect="auto")
plt.colorbar(im, ax=ax, label="Correlation Coefficient")

ax.set_xticks(range(len(corr_labels)))
ax.set_yticks(range(len(corr_labels)))
ax.set_xticklabels(corr_labels, rotation=45, ha="right", fontsize=10)
ax.set_yticklabels(corr_labels, fontsize=10)

for i in range(len(corr_labels)):
    for j in range(len(corr_labels)):
        val = corr_matrix.values[i, j]
        ax.text(j, i, f"{val:.2f}",
                ha="center", va="center", fontsize=9, fontweight="bold",
                color="black" if abs(val) < 0.7 else "white")

plt.tight_layout()
plt.savefig("data/outputs/05_macro_correlation_matrix.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 05_macro_correlation_matrix.png")

# =============================================================================
# SECTION 6: SUMMARY STATISTICS
# =============================================================================
print("\nSection 6: Summary Statistics...")

summary = master_full[[
    "aluminum_usd_mt", "lithium_usd_mt", "cobalt_usd_mt"
]].describe().round(2)

summary.index   = ["Count", "Mean", "Std Dev", "Min", "25th %ile",
                    "Median", "75th %ile", "Max"]
summary.columns = ["Aluminum ($/mt)", "Lithium ($/mt)", "Cobalt ($/mt)"]

print(summary.to_string())
summary.to_csv("data/outputs/summary_statistics.csv")
print("✅ Saved: summary_statistics.csv")

print("\n" + "=" * 60)
print("OUTPUTS SAVED TO data/outputs/:")
print("  01_commodity_price_trends.png")
print("  02_yoy_variance_analysis.png")
print("  03_volatility_analysis.png")
print("  04_structural_breaks_cusum.png")
print("  05_macro_correlation_matrix.png")
print("  annual_price_variance.csv")
print("  summary_statistics.csv")
print("=" * 60)
print(f"04 COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)
