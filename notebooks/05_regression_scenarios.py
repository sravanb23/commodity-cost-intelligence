# =============================================================================
# 05: MACRO REGRESSION + GEOPOLITICAL SCENARIO SIMULATOR
# Reads from: data/processed/commodity_master.csv
# Outputs:    data/outputs/ (charts + tables)
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error
import warnings
import os
from datetime import datetime
warnings.filterwarnings("ignore")

os.makedirs("data/outputs", exist_ok=True)

print("=" * 60)
print("05: MACRO REGRESSION + SCENARIO SIMULATOR")
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
df     = master.dropna().copy().reset_index(drop=True)

print(f"\n✅ Loaded: {master.shape[0]} rows")
print(f"   Clean regression dataset: {len(df)} rows")

FEATURES = [
    "wti_crude_usd",
    "natural_gas_usd",
    "industrial_production_idx",
    "vix",
    "usd_cny"
]

FEATURE_LABELS = [
    "WTI Crude",
    "Natural Gas",
    "Industrial Prod",
    "VIX",
    "USD/CNY"
]

TARGETS = [
    ("aluminum_usd_mt", "Aluminum"),
    ("lithium_usd_mt",  "Lithium"),
    ("cobalt_usd_mt",   "Cobalt"),
]

COLORS = ["#2196F3", "#4CAF50", "#FF5722"]

# =============================================================================
# SECTION 1: OLS MACRO REGRESSION
# =============================================================================
print("\n" + "=" * 60)
print("Section 1: OLS Macro Regression...")
print("=" * 60)

scaler = StandardScaler()
X      = df[FEATURES].values
X_scaled = scaler.fit_transform(X)

regression_results = {}

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle(
    "OLS Macro Regression — Feature Coefficients\n"
    "Quantifying Macro Drivers of Commodity Prices",
    fontsize=14, fontweight="bold"
)

for ax, (target_col, target_name), color in zip(axes, TARGETS, COLORS):
    y     = df[target_col].values
    model = LinearRegression()
    model.fit(X_scaled, y)
    y_pred = model.predict(X_scaled)

    r2  = r2_score(y, y_pred)
    mae = mean_absolute_error(y, y_pred)

    regression_results[target_col] = {
        "model":        model,
        "r2":           r2,
        "mae":          mae,
        "coefficients": dict(zip(FEATURE_LABELS, model.coef_)),
        "intercept":    model.intercept_
    }

    print(f"\n{target_name} Regression:")
    print(f"  R² = {r2:.3f} | MAE = ${mae:,.0f}/mt")
    for feat, coef in zip(FEATURE_LABELS, model.coef_):
        print(f"  {feat:25s}: {coef:+,.1f}")

    bar_colors = ["#2196F3" if c > 0 else "#EF5350" for c in model.coef_]
    bars = ax.barh(FEATURE_LABELS, model.coef_, color=bar_colors, alpha=0.85)
    ax.axvline(x=0, color="black", linewidth=0.8)
    ax.set_title(
        f"{target_name}\nR²={r2:.3f} | MAE=${mae:,.0f}/mt",
        fontsize=11, fontweight="bold"
    )
    ax.set_xlabel("Coefficient (Standardized)", fontsize=9)
    ax.grid(True, alpha=0.3, axis="x")
    for bar, val in zip(bars, model.coef_):
        ax.text(
            val + max(abs(model.coef_)) * 0.03,
            bar.get_y() + bar.get_height() / 2,
            f"{val:+,.0f}", va="center", fontsize=8
        )

plt.tight_layout()
plt.savefig("data/outputs/06_macro_regression_coefficients.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("\n✅ Saved: 06_macro_regression_coefficients.png")

# =============================================================================
# SECTION 2: ACTUAL VS PREDICTED
# =============================================================================
print("\nSection 2: Actual vs Predicted...")

fig, axes = plt.subplots(3, 1, figsize=(16, 12))
fig.suptitle(
    "Macro Regression — Actual vs Predicted Commodity Prices",
    fontsize=14, fontweight="bold"
)

for ax, (target_col, target_name), color in zip(axes, TARGETS, COLORS):
    model  = regression_results[target_col]["model"]
    y      = df[target_col].values
    y_pred = model.predict(X_scaled)
    r2     = regression_results[target_col]["r2"]
    mae    = regression_results[target_col]["mae"]

    ax.plot(df["date"], y,      color=color,   linewidth=1.8,
            label="Actual", alpha=0.9)
    ax.plot(df["date"], y_pred, color="black", linewidth=1.2,
            linestyle="--", alpha=0.7, label="Predicted")
    ax.fill_between(df["date"], y, y_pred,
                    alpha=0.15, color="red", label="Residual")
    ax.set_title(f"{target_name} — R²={r2:.3f}, MAE=${mae:,.0f}/mt",
                 fontsize=10)
    ax.set_ylabel("Price ($/mt)", fontsize=9)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

axes[-1].set_xlabel("Date", fontsize=10)
plt.tight_layout()
plt.savefig("data/outputs/07_actual_vs_predicted.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("✅ Saved: 07_actual_vs_predicted.png")

# =============================================================================
# SECTION 3: EVENT STUDY ANALYSIS
# =============================================================================
print("\nSection 3: Event Study Analysis...")

EVENTS = {
    "2008-09": {"label": "Financial Crisis",   "duration_months": 12},
    "2020-03": {"label": "COVID-19",           "duration_months": 6},
    "2021-06": {"label": "EV/Battery Boom",    "duration_months": 12},
    "2022-02": {"label": "Russia-Ukraine War", "duration_months": 9},
    "2022-10": {"label": "China Lockdowns",    "duration_months": 6},
}

event_responses = {}

for event_date, event_info in EVENTS.items():
    ed       = pd.to_datetime(event_date)
    duration = event_info["duration_months"]
    label    = event_info["label"]

    pre_start = ed - pd.DateOffset(months=3)
    post_end  = ed + pd.DateOffset(months=duration)

    pre_data  = df[df["date"].between(pre_start, ed)]
    post_data = df[df["date"].between(ed, post_end)]

    if len(pre_data) == 0 or len(post_data) == 0:
        continue

    response = {}
    for target_col, target_name in TARGETS:
        pre_mean   = pre_data[target_col].mean()
        post_mean  = post_data[target_col].mean()
        pct_change = ((post_mean - pre_mean) / pre_mean) * 100
        response[target_name] = {
            "pre_mean":   pre_mean,
            "post_mean":  post_mean,
            "pct_change": pct_change
        }

    event_responses[label] = response
    print(f"\n{label} ({event_date}):")
    for comm, data in response.items():
        print(f"  {comm:10s}: {data['pct_change']:+.1f}% "
              f"(${data['pre_mean']:,.0f} → ${data['post_mean']:,.0f}/mt)")

# Event study chart
fig, ax = plt.subplots(figsize=(14, 7))
fig.suptitle(
    "Event Study — Historical Price Response to Geopolitical Shocks (%)\n"
    "Basis for Dynamic Scenario Simulation",
    fontsize=14, fontweight="bold"
)

event_names     = list(event_responses.keys())
commodity_names = ["Aluminum", "Lithium", "Cobalt"]
x     = np.arange(len(event_names))
width = 0.25

for i, (comm, color) in enumerate(zip(commodity_names, COLORS)):
    values = [
        event_responses[e][comm]["pct_change"]
        for e in event_names
        if comm in event_responses[e]
    ]
    bars = ax.bar(x + i * width, values, width,
                  label=comm, color=color, alpha=0.85, edgecolor="white")
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + (1 if val >= 0 else -5),
            f"{val:+.0f}%",
            ha="center", va="bottom", fontsize=8, fontweight="bold"
        )

ax.axhline(y=0, color="black", linewidth=0.8)
ax.set_xticks(x + width)
ax.set_xticklabels(event_names, rotation=15, ha="right", fontsize=10)
ax.set_ylabel("Average Price Change (%)", fontsize=10)
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3, axis="y")
plt.tight_layout()
plt.savefig("data/outputs/08_event_study_responses.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("\n✅ Saved: 08_event_study_responses.png")

# =============================================================================
# SECTION 4: GEOPOLITICAL SCENARIO SIMULATOR
# Multipliers dynamically calculated from event study results
# =============================================================================
print("\nSection 4: Geopolitical Scenario Simulator...")

# Current prices (latest available)
current_prices = {
    "Aluminum": df["aluminum_usd_mt"].iloc[-1],
    "Lithium":  df["lithium_usd_mt"].iloc[-1],
    "Cobalt":   df["cobalt_usd_mt"].iloc[-1],
}

print(f"\nCurrent baseline prices:")
for comm, price in current_prices.items():
    print(f"  {comm:10s}: ${price:,.0f}/mt")

def get_multiplier(event_label, commodity, event_responses):
    if event_label not in event_responses:
        return 1.0
    if commodity not in event_responses[event_label]:
        return 1.0
    pct_change = event_responses[event_label][commodity]["pct_change"]
    multiplier = 1 + (pct_change / 100)
    return round(max(0.30, min(multiplier, 3.0)), 4)

SCENARIO_DEFINITIONS = {
    "Strait of Hormuz\nClosure": {
        "analogue":    "Russia-Ukraine War",
        "description": "Energy shock → smelting cost spike"
    },
    "China Supply\nRestriction": {
        "analogue":    "China Lockdowns",
        "description": "Supply disruption → processing bottleneck"
    },
    "Global\nRecession": {
        "analogue":    "Financial Crisis",
        "description": "Demand collapse → price decline"
    },
    "EV Demand\nSurge": {
        "analogue":    "EV/Battery Boom",
        "description": "Battery material demand surge"
    },
    "Base Case\n(No Shock)": {
        "analogue":    None,
        "description": "Current prices held flat"
    },
}

print(f"\nDynamically calculated scenario multipliers:")
print(f"  {'Scenario':<35} {'Analogue':<25} "
      f"{'Aluminum':>10} {'Lithium':>10} {'Cobalt':>10}")
print(f"  {'-'*35} {'-'*25} {'-'*10} {'-'*10} {'-'*10}")

scenarios = {}
for scenario_name, scenario_def in SCENARIO_DEFINITIONS.items():
    analogue = scenario_def["analogue"]
    if analogue is None:
        multipliers = {"Aluminum": 1.0, "Lithium": 1.0, "Cobalt": 1.0}
    else:
        multipliers = {
            comm: get_multiplier(analogue, comm, event_responses)
            for comm in ["Aluminum", "Lithium", "Cobalt"]
        }
    scenarios[scenario_name] = {
        "analogue":    analogue,
        "description": scenario_def["description"],
        "adjustments": multipliers
    }
    clean_name    = scenario_name.replace("\n", " ")
    clean_analogue = analogue if analogue else "None"
    print(f"  {clean_name:<35} {clean_analogue:<25} "
          f"{multipliers['Aluminum']:>10.4f} "
          f"{multipliers['Lithium']:>10.4f} "
          f"{multipliers['Cobalt']:>10.4f}")

# Calculate scenario prices
scenario_results = {}
for scenario_name, scenario_data in scenarios.items():
    scenario_prices = {}
    for comm, current in current_prices.items():
        adj = scenario_data["adjustments"][comm]
        scenario_prices[comm] = {
            "base":       current,
            "shocked":    current * adj,
            "pct_change": (adj - 1) * 100,
            "abs_change": current * adj - current
        }
    scenario_results[scenario_name] = scenario_prices

print(f"\nScenario Simulation Results:")
print(f"  {'Scenario':<30} {'Commodity':<12} "
      f"{'Base':>10} {'Shocked':>10} {'Change':>10}")
print(f"  {'-'*30} {'-'*12} {'-'*10} {'-'*10} {'-'*10}")
for scenario_name, comm_data in scenario_results.items():
    for comm, prices in comm_data.items():
        clean_name = scenario_name.replace("\n", " ")
        print(f"  {clean_name:<30} {comm:<12} "
              f"${prices['base']:>9,.0f} "
              f"${prices['shocked']:>9,.0f} "
              f"{prices['pct_change']:>+9.1f}%")

# Save scenario results
scenario_rows = []
for scenario_name, comm_data in scenario_results.items():
    for comm, prices in comm_data.items():
        scenario_rows.append({
            "scenario":      scenario_name.replace("\n", " "),
            "commodity":     comm,
            "analogue":      scenarios[scenario_name]["analogue"],
            "base_price":    prices["base"],
            "shocked_price": prices["shocked"],
            "pct_change":    prices["pct_change"],
            "abs_change":    prices["abs_change"]
        })
pd.DataFrame(scenario_rows).to_csv(
    "data/outputs/scenario_simulation_results.csv", index=False
)

# Scenario simulator chart
scenario_names = list(scenarios.keys())
fig, axes = plt.subplots(1, 3, figsize=(18, 7))
fig.suptitle(
    "Geopolitical Scenario Simulator — Projected Commodity Prices\n"
    "Multipliers Dynamically Derived from Historical Event Study",
    fontsize=14, fontweight="bold"
)

x = np.arange(len(scenario_names))
for ax, comm, color in zip(axes, ["Aluminum", "Lithium", "Cobalt"], COLORS):
    prices = [scenario_results[s][comm]["shocked"] for s in scenario_names]
    bar_colors = [
        "#EF5350" if p > current_prices[comm]
        else "#66BB6A" if p < current_prices[comm]
        else "#90A4AE"
        for p in prices
    ]
    bars = ax.bar(x, prices, color=bar_colors, alpha=0.85,
                  edgecolor="white", width=0.6)
    ax.axhline(
        y=current_prices[comm], color="black",
        linewidth=1.5, linestyle="--",
        label=f"Current: ${current_prices[comm]:,.0f}"
    )
    for bar, price, s_name in zip(bars, prices, scenario_names):
        pct = scenario_results[s_name][comm]["pct_change"]
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + current_prices[comm] * 0.01,
            f"${price:,.0f}\n({pct:+.0f}%)",
            ha="center", va="bottom", fontsize=7, fontweight="bold"
        )
    ax.set_title(f"{comm}", fontsize=12, fontweight="bold")
    ax.set_ylabel("Price ($/mt)", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(scenario_names, fontsize=7)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig("data/outputs/09_geopolitical_scenario_simulator.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("\n✅ Saved: 09_geopolitical_scenario_simulator.png")

# =============================================================================
# SECTION 5: PPV COST IMPACT MODEL
# =============================================================================
print("\nSection 5: PPV Cost Impact Simulation...")

ANNUAL_VOLUME_MT = {
    "Aluminum": 50000,
    "Lithium":  5000,
    "Cobalt":   2000,
}

print(f"\nAnnual procurement volume assumptions:")
for comm, vol in ANNUAL_VOLUME_MT.items():
    print(f"  {comm:10s}: {vol:,} MT/year")

print(f"\nPPV Cost Impact by Scenario (Annual):")
print(f"  {'Scenario':<30} {'Commodity':<12} "
      f"{'Base Cost':>14} {'Shocked Cost':>14} {'PPV Impact':>14}")
print(f"  {'-'*30} {'-'*12} {'-'*14} {'-'*14} {'-'*14}")

ppv_rows = []
for scenario_name, comm_data in scenario_results.items():
    for comm, prices in comm_data.items():
        vol        = ANNUAL_VOLUME_MT[comm]
        base_cost  = prices["base"]   * vol
        shock_cost = prices["shocked"] * vol
        ppv        = shock_cost - base_cost
        clean_name = scenario_name.replace("\n", " ")
        print(f"  {clean_name:<30} {comm:<12} "
              f"${base_cost/1e6:>12.1f}M "
              f"${shock_cost/1e6:>12.1f}M "
              f"${ppv/1e6:>+12.1f}M")
        ppv_rows.append({
            "scenario":         clean_name,
            "commodity":        comm,
            "volume_mt":        vol,
            "base_cost_usd":    base_cost,
            "shocked_cost_usd": shock_cost,
            "ppv_impact_usd":   ppv
        })

ppv_df = pd.DataFrame(ppv_rows)
ppv_df.to_csv("data/outputs/ppv_cost_impact.csv", index=False)

# PPV chart
total_ppv = (
    ppv_df.groupby("scenario")["ppv_impact_usd"].sum() / 1e6
)
total_ppv = total_ppv.reindex(
    [s.replace("\n", " ") for s in scenario_names]
)

fig, ax = plt.subplots(figsize=(16, 7))
fig.suptitle(
    "Purchase Price Variance (PPV) — Total Annual Cost Impact by Scenario\n"
    "Based on Procurement Volume Assumptions",
    fontsize=14, fontweight="bold"
)

bar_colors = [
    "#EF5350" if v > 0 else "#66BB6A" if v < 0 else "#90A4AE"
    for v in total_ppv.values
]
bars = ax.bar(range(len(total_ppv)), total_ppv.values,
              color=bar_colors, alpha=0.85, edgecolor="white", width=0.6)
ax.axhline(y=0, color="black", linewidth=0.8)
ax.set_xticks(range(len(total_ppv)))
ax.set_xticklabels(
    [s.replace("\n", " ") for s in scenario_names],
    rotation=15, ha="right", fontsize=10
)
ax.set_ylabel("Total PPV Impact ($M)", fontsize=11)
ax.grid(True, alpha=0.3, axis="y")

for bar, val in zip(bars, total_ppv.values):
    if pd.notna(val):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(abs(total_ppv.dropna())) * 0.01,
            f"${val:+.1f}M",
            ha="center", va="bottom", fontsize=11, fontweight="bold"
        )

plt.tight_layout()
plt.savefig("data/outputs/10_ppv_cost_impact.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("\n✅ Saved: 10_ppv_cost_impact.png")

print("\n" + "=" * 60)
print("OUTPUTS SAVED TO data/outputs/:")
print("  06_macro_regression_coefficients.png")
print("  07_actual_vs_predicted.png")
print("  08_event_study_responses.png")
print("  09_geopolitical_scenario_simulator.png")
print("  10_ppv_cost_impact.png")
print("  scenario_simulation_results.csv")
print("  ppv_cost_impact.csv")
print("=" * 60)
print(f"05 COMPLETE ✅  [{datetime.now().strftime('%H:%M:%S')}]")
print("=" * 60)
