# Supply Chain PO Analytics & Reporting Pipeline

> An end-to-end Python automation pipeline that ingests purchase order data, computes supply chain KPIs, flags at-risk open orders, and auto-generates a professional multi-sheet Excel report — all from a single command.

---

## Business Problem

Procurement and operations teams lose significant time manually compiling supplier performance reports from raw PO exports. Without consistent KPI tracking, delayed orders go unnoticed until they impact production, and unreliable suppliers are identified only after problems escalate.

This pipeline solves that by automating the full analytics workflow: from raw data ingestion to a board-ready Excel report with supplier scorecards, trend analysis, and an at-risk order watchlist — ready to send to stakeholders with zero manual formatting.

---

## What This Pipeline Does

- **Generates** 1,500 realistic purchase orders across 25 suppliers with tier-based lead time and delay patterns
- **Cleans** raw data: enforces types, imputes missing costs, removes duplicates, validates referential integrity
- **Computes KPIs** at portfolio, supplier, and monthly levels — including a composite Reliability Score
- **Flags at-risk open orders** using a tiered scoring engine (CRITICAL / HIGH / MEDIUM)
- **Generates a 5-sheet Excel report** with conditional formatting, embedded charts, and a KPI scorecard
- **Exports CSVs** for downstream use in Power BI, Tableau, or further analysis

---

## Architecture

```
data_generator.py
      │  (raw orders + suppliers DataFrames)
      ▼
  pipeline.py  ──────────────────────────────┐
      │  (enriched orders, supplier KPIs,     │
      │   monthly KPIs, portfolio KPIs)       │
      ▼                                       ▼
 risk_engine.py                      report_generator.py
      │  (at-risk open orders)                │
      └──────────────┬────────────────────────┘
                     ▼
              exporter.py
         (CSVs + CLI summary table)
```

---

## Output Preview

After running the pipeline, `data/output/` contains:

| File | Description |
|---|---|
| `PO_Analytics_Report_<timestamp>.xlsx` | 5-sheet Excel workbook with charts |
| `orders_enriched_<timestamp>.csv` | Full enriched order dataset |
| `kpi_summary_<timestamp>.csv` | Supplier-level KPI table |
| `at_risk_orders_<timestamp>.csv` | At-risk open orders |

### Excel Report Sheets

| Sheet | Contents |
|---|---|
| **Executive Summary** | Portfolio KPI scorecard (RAG), spend by supplier chart, spend by category donut |
| **Supplier Performance** | All supplier KPIs with reliability heatmap and ranked bar chart |
| **Monthly Trends** | Dual-axis combo chart (spend + OTD rate), delay cost trend line |
| **At-Risk Open Orders** | Risk-flagged orders colour-coded by severity |
| **Raw Data** | Full enriched dataset with auto-filter |

---

## Key KPIs Computed

### Portfolio Level
| KPI | Description |
|---|---|
| On-Time Delivery (OTD) Rate | % of delivered orders that arrived on or before promised date |
| Avg Lead Time | Mean days from order to delivery across all delivered POs |
| Total Delay Cost | Financial exposure from late deliveries (0.2% of PO value per day late) |
| Delay Cost % of Spend | Delay cost as a fraction of total procurement spend |
| At-Risk Order Count | Open orders flagged HIGH or CRITICAL by the risk engine |

### Supplier Level
| KPI | Formula |
|---|---|
| OTD Rate | `on_time_deliveries / total_deliveries` |
| Lead Time Std Dev | Measures consistency, not just average speed |
| **Reliability Score** | `0.6 × OTD_rate + 0.4 × (1 − lead_time_CV)` |
| Delay Cost / Spend | Supplier-specific financial exposure ratio |

The **Reliability Score** rewards both timeliness and consistency — a supplier who is always 2 days late scores lower than one who is sometimes 5 days early and sometimes on time, even if their average is identical.

---

## Project Structure

```
supply-chain-po-analytics/
├── README.md
├── requirements.txt
├── run_pipeline.py          # Single entry point
├── config.py                # All constants and thresholds
├── data/
│   └── output/              # Generated files (git-ignored)
├── src/
│   ├── data_generator.py    # Synthetic PO + supplier data
│   ├── pipeline.py          # Cleaning, enrichment, KPI computation
│   ├── risk_engine.py       # At-risk order detection
│   ├── report_generator.py  # Excel report with openpyxl
│   └── exporter.py          # CSV exports + CLI summary
└── tests/
    ├── test_pipeline.py
    └── test_risk_engine.py
```

---

## How to Run

### Prerequisites
- Python 3.9+

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the full pipeline
```bash
python run_pipeline.py
```

### Run tests
```bash
pytest tests/ -v
```

---

## Configuration

All simulation parameters and business thresholds live in `config.py`:

| Parameter | Default | Description |
|---|---|---|
| `NUM_ORDERS` | 1500 | Number of POs to simulate |
| `NUM_SUPPLIERS` | 25 | Number of unique suppliers |
| `SIMULATION_START_DATE` | 2023-01-01 | Earliest order date |
| `SIMULATION_END_DATE` | 2024-12-31 | Latest order date |
| `DELAY_COST_RATE_PER_DAY` | 0.002 | Cost penalty per day late (as % of PO value) |
| `AT_RISK_THRESHOLD_DAYS` | 3 | Days to promised date that triggers MEDIUM risk |
| `SUPPLIER_RELIABILITY_FLOOR` | 0.75 | Reliability score below which a supplier is flagged |

To use real data instead of synthetic, replace the `data_generator.generate()` call in `run_pipeline.py` with your own CSV/database ingestion function that returns `(orders_df, suppliers_df)` matching the expected schema.

---

## Tech Stack

| Library | Purpose |
|---|---|
| `pandas` | Data transformation and KPI aggregation |
| `numpy` | Statistical calculations, random data generation |
| `openpyxl` | Excel report generation with embedded charts |
| `faker` | Realistic supplier company names |
| `tabulate` | Formatted CLI summary output |
| `pytest` | Unit and integration tests |

---

## Business Context & Assumptions

**Supplier tiers** model the real-world pattern where strategic Tier 1 suppliers have tighter SLAs and more reliable delivery than lower-tier vendors. Tier 3 suppliers have a 30% delay probability vs 5% for Tier 1.

**Delay cost model:** The 0.2%/day rate approximates blended carrying costs, expediting fees, and production disruption. This is a simplified model — a production implementation would use actual penalty clauses from contracts.

**Reliability Score formula:** The CV (coefficient of variation = std/mean) component penalises unpredictable suppliers even if their average lead time is acceptable. Predictability matters for production planning as much as raw speed.

**At-risk detection:** Uses supplier historical average lead time to project expected delivery for open orders, comparing against both the contractual promised date and the supplier's reliability flag.

---

## Potential Extensions

- **Live data connection:** Replace `data_generator` with a connector to SAP, Oracle, or an ERP CSV export
- **Scheduled automation:** Run via `cron` or Apache Airflow on a weekly cadence
- **Email delivery:** Add `smtplib` to email the report to procurement leads automatically
- **BI tool integration:** The exported CSVs connect directly to Power BI or Tableau
- **Alerting:** Post high-risk order summaries to a Slack channel via webhook
