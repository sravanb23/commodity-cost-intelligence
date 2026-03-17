# =============================================================================
# run_analysis.py — ANALYSIS PIPELINE ORCHESTRATOR
# Runs EDA and regression/scenario analysis in order:
#   04 → 05
# Requires master dataset to exist (run run_ingestion.py first)
# =============================================================================

import subprocess
import sys
import os
import json
from datetime import datetime

# =============================================================================
# PIPELINE DEFINITION
# =============================================================================
PIPELINE = [
    {
        "id":       "04",
        "label":    "EDA + Structural Break Detection",
        "script":   "notebooks/04_eda_analysis.py",
        "produces": [
            "data/outputs/01_commodity_price_trends.png",
            "data/outputs/02_yoy_variance_analysis.png",
            "data/outputs/03_volatility_analysis.png",
            "data/outputs/04_structural_breaks_cusum.png",
            "data/outputs/05_macro_correlation_matrix.png",
            "data/outputs/annual_price_variance.csv",
            "data/outputs/summary_statistics.csv"
        ],
        "requires": ["data/processed/commodity_master.csv"]
    },
    {
        "id":       "05",
        "label":    "Macro Regression + Scenario Simulator",
        "script":   "notebooks/05_regression_scenarios.py",
        "produces": [
            "data/outputs/06_macro_regression_coefficients.png",
            "data/outputs/07_actual_vs_predicted.png",
            "data/outputs/08_event_study_responses.png",
            "data/outputs/09_geopolitical_scenario_simulator.png",
            "data/outputs/10_ppv_cost_impact.png",
            "data/outputs/scenario_simulation_results.csv",
            "data/outputs/ppv_cost_impact.csv"
        ],
        "requires": ["data/processed/commodity_master.csv"]
    },
]

# =============================================================================
# HELPERS
# =============================================================================
def separator(char="=", width=60):
    print(char * width)

def run_step(step):
    start  = datetime.now()
    result = subprocess.run(
        [sys.executable, step["script"]],
        capture_output=False
    )
    duration = (datetime.now() - start).total_seconds()
    success  = result.returncode == 0
    return success, duration

def check_dependencies(step):
    return [req for req in step["requires"] if not os.path.exists(req)]

def check_outputs(step):
    return [out for out in step["produces"] if not os.path.exists(out)]

# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================
def main():
    separator()
    print("  COMMODITY COST INTELLIGENCE — ANALYSIS PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    separator()

    # Check master dataset exists before anything else
    master_path = "data/processed/commodity_master.csv"
    if not os.path.exists(master_path):
        print(f"\n  ❌ Master dataset not found: {master_path}")
        print(f"     Run the ingestion pipeline first:")
        print(f"     python run_ingestion.py")
        return 1

    import pandas as pd
    master = pd.read_csv(master_path, parse_dates=["date"])
    print(f"\n  Master dataset: {len(master)} rows | "
          f"{master['date'].min().date()} → {master['date'].max().date()}")
    print(f"  Pipeline: {len(PIPELINE)} steps\n")

    results        = []
    pipeline_start = datetime.now()

    for step in PIPELINE:
        separator("-")
        print(f"  STEP {step['id']}: {step['label']}")
        separator("-")

        # Check dependencies
        missing_deps = check_dependencies(step)
        if missing_deps:
            print(f"\n  ❌ DEPENDENCY ERROR:")
            for dep in missing_deps:
                print(f"     — {dep}")
            results.append({
                "step":     step["id"],
                "label":    step["label"],
                "status":   "DEPENDENCY_ERROR",
                "duration": 0
            })
            break

        # Run step
        print(f"\n  Running: {step['script']}\n")
        success, duration = run_step(step)

        # Check outputs
        if success:
            missing_outputs = check_outputs(step)
            if missing_outputs:
                print(f"\n  ⚠️  Missing outputs:")
                for out in missing_outputs:
                    print(f"     — {out}")

        status = "✅ SUCCESS" if success else "❌ FAILED"
        results.append({
            "step":     step["id"],
            "label":    step["label"],
            "status":   "SUCCESS" if success else "FAILED",
            "duration": round(duration, 1)
        })

        print(f"\n  {status} — {step['id']}: {step['label']} "
              f"({duration:.1f}s)")

        if not success:
            print(f"\n  Pipeline stopped at step {step['id']}.")
            break

    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    total_duration = (datetime.now() - pipeline_start).total_seconds()
    all_success    = all(r["status"] == "SUCCESS" for r in results)

    separator()
    print("  ANALYSIS PIPELINE SUMMARY")
    separator()

    print(f"\n  {'Step':<6} {'Label':<45} {'Status':<12} {'Time':>6}")
    print(f"  {'-'*6} {'-'*45} {'-'*12} {'-'*6}")
    for r in results:
        icon = "✅" if r["status"] == "SUCCESS" else "❌"
        print(f"  {r['step']:<6} {r['label']:<45} "
              f"{icon} {r['status']:<10} {r['duration']:>5.1f}s")

    print(f"\n  Total time : {total_duration:.1f}s")

    if all_success and len(results) == len(PIPELINE):
        print(f"\n  ✅ ANALYSIS PIPELINE COMPLETE")
        print(f"     Charts saved to : data/outputs/")
        print(f"     Tables saved to : data/outputs/")
    else:
        print(f"\n  ❌ PIPELINE INCOMPLETE — see errors above")

    separator()

    # Save log
    log = {
        "pipeline":       "analysis",
        "run_at":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_duration": round(total_duration, 1),
        "status":         "SUCCESS" if all_success else "FAILED",
        "steps":          results
    }
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/analysis_log.json", "w") as f:
        json.dump(log, f, indent=2)

    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
