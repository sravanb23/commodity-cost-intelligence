# =============================================================================
# run_ingestion.py — DATA INGESTION ORCHESTRATOR
# Runs the full data ingestion pipeline in order:
#   01a → 01b → 01c → 01d → 02 → 03
# Each step checks dependencies before running.
# Stop on first failure and report exactly what went wrong.
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
        "id":           "01a",
        "label":        "Aluminum Ingestion (World Bank)",
        "script":       "notebooks/01a_ingest_aluminum.py",
        "produces":     ["data/warehouse/aluminum_worldbank.csv"],
        "requires":     []
    },
    {
        "id":           "01b",
        "label":        "Lithium Ingestion (USGS + IMF)",
        "script":       "notebooks/01b_ingest_lithium.py",
        "produces":     [
                            "data/warehouse/lithium_usgs.csv",
                            "data/warehouse/lithium_imf_raw.csv",
                            "data/warehouse/lithium_combined.csv"
                        ],
        "requires":     [
                            "data/raw/usgs_lithium.xlsx",
                            "data/raw/imf_commodity_prices.csv"
                        ]
    },
    {
        "id":           "01c",
        "label":        "Cobalt Ingestion (IMF Monthly USD)",
        "script":       "notebooks/01c_ingest_cobalt.py",
        "produces":     [
                            "data/warehouse/cobalt_imf_monthly.csv",
                            "data/warehouse/cobalt_usgs.csv"
                        ],
        "requires":     [
                            "data/raw/usgs_cobalt.xlsx",
                            "data/raw/imf_commodity_prices.csv"
                        ]
    },
    {
        "id":           "01d",
        "label":        "Macro Variables (FRED + Yahoo Finance)",
        "script":       "notebooks/01d_ingest_macro.py",
        "produces":     [
                            "data/warehouse/macro_fred.csv",
                            "data/warehouse/vix_yahoo.csv"
                        ],
        "requires":     []
    },
    {
        "id":           "02",
        "label":        "Warehouse Validation & Health Check",
        "script":       "notebooks/02_build_warehouse.py",
        "produces":     ["data/processed/warehouse_summary.json"],
        "requires":     [
                            "data/warehouse/aluminum_worldbank.csv",
                            "data/warehouse/lithium_combined.csv",
                            "data/warehouse/cobalt_imf_monthly.csv",
                            "data/warehouse/macro_fred.csv",
                            "data/warehouse/vix_yahoo.csv"
                        ]
    },
    {
        "id":           "03",
        "label":        "Master Dataset Build",
        "script":       "notebooks/03_build_master.py",
        "produces":     ["data/processed/commodity_master.csv"],
        "requires":     ["data/processed/warehouse_summary.json"]
    },
]

# =============================================================================
# HELPERS
# =============================================================================
def separator(char="=", width=60):
    print(char * width)

def run_step(step):
    """Run a pipeline step and return (success, duration_seconds)"""
    start = datetime.now()

    result = subprocess.run(
        [sys.executable, step["script"]],
        capture_output=False
    )

    duration = (datetime.now() - start).total_seconds()
    success  = result.returncode == 0

    return success, duration

def check_dependencies(step):
    """Check all required files exist before running step"""
    missing = [
        req for req in step["requires"]
        if not os.path.exists(req)
    ]
    return missing

def check_outputs(step):
    """Check all expected outputs were produced"""
    missing = [
        out for out in step["produces"]
        if not os.path.exists(out)
    ]
    return missing

# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================
def main():
    separator()
    print("  COMMODITY COST INTELLIGENCE — INGESTION PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    separator()
    print(f"\n  Pipeline: {len(PIPELINE)} steps")
    for step in PIPELINE:
        print(f"    {step['id']:>4} — {step['label']}")
    print()

    results  = []
    pipeline_start = datetime.now()

    for i, step in enumerate(PIPELINE):
        separator("-")
        print(f"  STEP {step['id']}: {step['label']}")
        separator("-")

        # --- Check dependencies ---
        missing_deps = check_dependencies(step)
        if missing_deps:
            print(f"\n  ❌ DEPENDENCY ERROR — missing required files:")
            for dep in missing_deps:
                print(f"     — {dep}")
            print(f"\n  Pipeline stopped at step {step['id']}.")
            results.append({
                "step":    step["id"],
                "label":   step["label"],
                "status":  "DEPENDENCY_ERROR",
                "duration": 0
            })
            break

        # --- Run step ---
        print(f"\n  Running: {step['script']}\n")
        success, duration = run_step(step)

        # --- Check outputs ---
        if success:
            missing_outputs = check_outputs(step)
            if missing_outputs:
                print(f"\n  ⚠️  Step completed but outputs missing:")
                for out in missing_outputs:
                    print(f"     — {out}")
                success = False

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
            print(f"  Fix the error above and re-run.")
            break

    # ==========================================================================
    # PIPELINE SUMMARY
    # ==========================================================================
    total_duration = (datetime.now() - pipeline_start).total_seconds()

    separator()
    print("  PIPELINE SUMMARY")
    separator()

    all_success = all(r["status"] == "SUCCESS" for r in results)

    print(f"\n  {'Step':<6} {'Label':<40} {'Status':<12} {'Time':>6}")
    print(f"  {'-'*6} {'-'*40} {'-'*12} {'-'*6}")
    for r in results:
        icon = "✅" if r["status"] == "SUCCESS" else "❌"
        print(f"  {r['step']:<6} {r['label']:<40} "
              f"{icon} {r['status']:<10} {r['duration']:>5.1f}s")

    print(f"\n  Total time : {total_duration:.1f}s")
    print(f"  Steps run  : {len(results)}/{len(PIPELINE)}")

    if all_success and len(results) == len(PIPELINE):
        print(f"\n  ✅ INGESTION PIPELINE COMPLETE")
        print(f"     Master dataset ready at: "
              f"data/processed/commodity_master.csv")
        print(f"     Run analysis pipeline with: "
              f"python run_analysis.py")
    else:
        print(f"\n  ❌ PIPELINE INCOMPLETE — see errors above")

    separator()

    # Save pipeline log
    log = {
        "pipeline":       "ingestion",
        "run_at":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_duration": round(total_duration, 1),
        "status":         "SUCCESS" if all_success else "FAILED",
        "steps":          results
    }
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/ingestion_log.json", "w") as f:
        json.dump(log, f, indent=2)

    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
