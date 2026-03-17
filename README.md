# Electronics Commodity Cost Intelligence Platform
### Aluminum · Lithium · Cobalt — Price Forecasting, Geopolitical Scenario Simulation & PPV Analysis

---

## Overview

This project builds an end-to-end commodity cost intelligence platform designed to support procurement finance decision-making in high-volume electronics manufacturing environments. It ingests, cleans, and warehouses 25 years of commodity price data across aluminum, lithium, and cobalt, then applies macro regression modeling, geopolitical event study analysis, and purchase price variance (PPV) simulation to quantify cost exposure under multiple supply disruption scenarios.

The platform is structured around a production-grade data pipeline with a clean warehouse architecture, full data lineage documentation, and two orchestrators — one for ingestion, one for analysis — that can be run independently or in sequence.

---

## Project Architecture

```
commodity-cost-intelligence/
│
├── data/
│   ├── raw/               # Source files — never modified
│   ├── warehouse/         # Clean, source-specific tables (one per source)
│   ├── processed/         # Master analytical dataset + lineage metadata
│   └── outputs/           # Charts, scenario results, PPV tables
│
├── notebooks/
│   ├── 01a_ingest_aluminum.py
│   ├── 01b_ingest_lithium.py
│   ├── 01c_ingest_cobalt.py
│   ├── 01d_ingest_macro.py
│   ├── 02_build_warehouse.py
│   ├── 03_build_master.py
│   ├── 04_eda_analysis.py
│   └── 05_regression_scenarios.py
│
├── run_ingestion.py       # Orchestrator: 01a → 01b → 01c → 01d → 02 → 03
├── run_analysis.py        # Orchestrator: 04 → 05
└── requirements.txt
```

---

## Data Sources & Methodology

### Aluminum
| Property | Detail |
|---|---|
| Source | World Bank Pink Sheet (Monthly Prices) |
| Frequency | Monthly |
| Coverage | January 2000 — Present |
| Unit | USD per metric ton (LME standard grade) |
| Method | Live download from World Bank URL on each pipeline run |

### Lithium *(Battery Grade 99%)*
| Property | Detail |
|---|---|
| Pre-2012 source | USGS Mineral Commodity Summaries |
| Pre-2012 unit | USD/mt (lithium carbonate), scaled to battery grade |
| Pre-2012 method | Annual prices scaled to IMF battery grade equivalent via bridging factor (15.22×), then linearly interpolated to monthly |
| Post-2012 source | IMF Primary Commodity Price System (PCPS) |
| Post-2012 unit | USD/mt (99% battery grade lithium) |
| Post-2012 method | Direct monthly prices — no transformation |
| Splice date | June 2012 (IMF data begins) |
| Splice discontinuity | 0.9% — seamless transition |
| Coverage | January 2000 — February 2026 |

> **Methodology note:** The IMF series is treated as authoritative. Pre-2012 USGS lithium carbonate prices are scaled up to the IMF battery grade scale using a bridging factor calculated at the 2012 overlap point. This approach preserves the real monthly granularity of the IMF series while extending coverage back to 2000 using USGS government data. Pre-2012 lithium was a low-volatility industrial commodity; annual interpolation is appropriate for this period.

### Cobalt
| Property | Detail |
|---|---|
| Source | IMF Primary Commodity Price System (PCPS) |
| Frequency | Monthly |
| Coverage | January 2000 — February 2026 |
| Unit | USD per metric ton |
| Method | Direct monthly USD spot prices — no interpolation or reconstruction |
| Reference | USGS Mineral Commodity Summaries (warehouse only, not used in master) |

### Macro Variables
| Variable | Source | FRED Series | Notes |
|---|---|---|---|
| WTI Crude Oil | FRED | `DCOILWTICO` | Energy cost proxy for aluminum smelting |
| Natural Gas | FRED | `MHHNGSP` | Smelting and processing cost driver |
| Industrial Production | FRED | `IPMAN` | Manufacturing demand proxy |
| USD/CNY FX | FRED | `DEXCHUS` | China demand and export competitiveness |
| VIX | Yahoo Finance | `^VIX` | Geopolitical and market uncertainty |

---

## Warehouse Architecture

All warehouse tables follow a consistent schema:
```
date | value | unit | source | frequency | notes
```

This makes it straightforward to swap sources, compare series, and document data lineage. The master dataset pulls from primary warehouse tables; reference tables (USGS for cobalt, USGS original scale for lithium) are preserved in the warehouse for comparison.

### Warehouse Files
| File | Content | Primary/Reference |
|---|---|---|
| `aluminum_worldbank.csv` | World Bank monthly aluminum prices | Primary |
| `lithium_combined.csv` | USGS + IMF spliced lithium series | Primary |
| `lithium_usgs.csv` | USGS original carbonate prices | Reference |
| `lithium_imf_raw.csv` | IMF battery grade monthly prices | Reference |
| `cobalt_imf_monthly.csv` | IMF direct monthly cobalt prices | Primary |
| `cobalt_usgs.csv` | USGS annual cobalt prices | Reference |
| `macro_fred.csv` | All FRED macro series (wide format) | Primary |
| `vix_yahoo.csv` | VIX monthly average | Primary |

---

## Pipeline

### Ingestion Pipeline
Runs all data ingestion steps in sequence with dependency checking:

```bash
python run_ingestion.py
```

```
01a  Aluminum ingestion (World Bank)
01b  Lithium ingestion (USGS + IMF, Option C bridging)
01c  Cobalt ingestion (IMF direct monthly USD)
01d  Macro variables (FRED API + Yahoo Finance)
02   Warehouse validation & health check
03   Master dataset assembly
```

Each step checks its dependencies exist before running. If any step fails, the pipeline stops and reports exactly what went wrong.

### Analysis Pipeline
Runs EDA and regression/scenario analysis on the master dataset:

```bash
python run_analysis.py
```

```
04   EDA + structural break detection
05   Macro regression + geopolitical scenario simulation + PPV model
```

---

## Analysis Modules

### Module 04 — Exploratory Data Analysis

**Price Trend Analysis**
25-year price history for all three commodities with 12-month rolling average overlay and key geopolitical event annotations.

**Year-over-Year Variance Analysis**
Annual PPV-style analysis — percentage price change year over year, directly mirroring procurement finance reporting workflows. Notable findings:
- Aluminum: +45.1% in 2021 (post-COVID supply disruption)
- Lithium: +274.8% in 2022 (EV/battery demand surge)
- Cobalt: +54.1% in 2017 (DRC supply concentration risk)

**Volatility Analysis**
Rolling 12-month coefficient of variation normalized across all three commodities for comparability. Identifies periods of elevated procurement cost risk.

**Structural Break Detection (CUSUM)**
CUSUM test applied to each commodity series to statistically identify price regime changes — confirms event study dates with data.

**Macro Correlation Matrix**
Pearson correlation between all commodity prices and macro variables. Key findings:
- Aluminum R² with macro variables: 0.547 (WTI crude is strongest driver)
- Lithium R² with macro variables: 0.135 (driven by EV demand, not macro)
- Cobalt R² with macro variables: 0.335 (industrial production is strongest)

> The low R² for lithium is itself a valuable finding — it confirms that lithium pricing is fundamentally driven by EV adoption curves and long-term supply contracts rather than traditional macro conditions. This informed the decision to use event-study based scenario simulation rather than purely macro regression for lithium forecasting.

---

### Module 05 — Macro Regression + Scenario Simulator

**OLS Macro Regression**
Standardized OLS regression of each commodity price against five macro variables (WTI crude, natural gas, industrial production, VIX, USD/CNY). Coefficients quantify the directional and magnitude relationship between macro conditions and commodity prices.

**Event Study Analysis**
Historical price responses measured across five geopolitical shock events:

| Event | Period | Method |
|---|---|---|
| Financial Crisis | Sep 2008 | 3M pre vs 12M post |
| COVID-19 | Mar 2020 | 3M pre vs 6M post |
| EV/Battery Boom | Jun 2021 | 3M pre vs 12M post |
| Russia-Ukraine War | Feb 2022 | 3M pre vs 9M post |
| China Lockdowns | Oct 2022 | 3M pre vs 6M post |

**Geopolitical Scenario Simulator**
Scenario multipliers are dynamically calculated from event study results — not hardcoded. Each scenario maps to the most historically analogous shock event:

| Scenario | Historical Analogue | Rationale |
|---|---|---|
| Strait of Hormuz Closure | Russia-Ukraine War | Energy supply shock → smelting cost spike |
| China Supply Restriction | China Lockdowns | Processing bottleneck → supply disruption |
| Global Recession | Financial Crisis | Demand collapse → price decline |
| EV Demand Surge | EV/Battery Boom | Battery material demand surge |
| Base Case | — | Current prices held flat |

> This approach makes the simulator fully data-driven and traceable. Every multiplier can be validated against the event study results in the output tables.

**PPV Cost Impact Model**
Translates price shock scenarios into annual procurement cost impact using volume assumptions representative of a large electronics manufacturer:
- Aluminum: 50,000 MT/year
- Lithium: 5,000 MT/year
- Cobalt: 2,000 MT/year

---

## Setup & Installation

### Prerequisites
- Python 3.8+
- FRED API key (free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html))

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/commodity-cost-intelligence.git
cd commodity-cost-intelligence

# Create virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Add your FRED API key to `notebooks/01d_ingest_macro.py`:
```python
FRED_API_KEY = "your_32_character_key_here"
```

### Run the full pipeline

```bash
# Step 1 — Ingest all data and build master dataset
python run_ingestion.py

# Step 2 — Run EDA and scenario analysis
python run_analysis.py
```

Total runtime: approximately 45-60 seconds.

---

## Output Files

| File | Description |
|---|---|
| `data/processed/commodity_master.csv` | Final analytical dataset (312 rows × 12 columns) |
| `data/processed/data_lineage.json` | Full data lineage documentation |
| `data/processed/warehouse_summary.json` | Warehouse health report |
| `data/outputs/01–05_*.png` | EDA charts |
| `data/outputs/06–10_*.png` | Regression and scenario charts |
| `data/outputs/annual_price_variance.csv` | YoY variance table |
| `data/outputs/scenario_simulation_results.csv` | Scenario prices by commodity |
| `data/outputs/ppv_cost_impact.csv` | Annual PPV impact by scenario |

---

## Tech Stack

| Category | Tools |
|---|---|
| Data Engineering | Python, Pandas, NumPy |
| Data Sources | World Bank API, IMF PCPS, USGS, FRED API, Yahoo Finance |
| Statistical Modeling | Scikit-learn (OLS), CUSUM structural break detection |
| Visualization | Matplotlib |
| Cloud & Storage | AWS (referenced in professional experience) |
| Pipeline Orchestration | Custom Python orchestrators with dependency checking |

---

## Data Lineage & Assumptions

Full data lineage is documented programmatically in `data/processed/data_lineage.json`. Key assumptions are:

1. **Lithium bridging factor (15.22×):** Calculated at the June 2012 overlap between USGS carbonate prices and IMF battery grade prices. Applied to the entire pre-2012 USGS series to normalize to battery grade scale. Splice discontinuity: 0.9%.

2. **Pre-2012 lithium interpolation:** Annual USGS data interpolated linearly to monthly. Justified because lithium was a low-volatility industrial commodity before the EV adoption wave — within-year price movements were minimal and not material to analysis.

3. **Cobalt data:** IMF direct monthly USD spot prices used as primary source. USGS annual data retained in warehouse as reference. No interpolation or reconstruction required.

4. **Scenario multipliers:** Derived entirely from historical event study data — not calibrated to any forward-looking forecast. Represent what happened in analogous historical shocks, applied to current price levels.

5. **PPV volume assumptions:** Illustrative procurement volumes representative of a large electronics manufacturer. Not based on any specific company's actual procurement data.

---

*Built as part of a commodity finance analytics portfolio targeting procurement finance roles in high-volume electronics manufacturing.*
