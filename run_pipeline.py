"""
Supply Chain PO Analytics Pipeline — Entry Point

Run with:
    python run_pipeline.py

Outputs written to data/output/:
    PO_Analytics_Report_<timestamp>.xlsx
    orders_enriched_<timestamp>.csv
    kpi_summary_<timestamp>.csv
    at_risk_orders_<timestamp>.csv
"""

import sys
import time
from datetime import datetime

# Allow importing from src/ without package install
sys.path.insert(0, "src")

import data_generator
import pipeline
import risk_engine
import report_generator
import exporter


def _log(msg: str, elapsed=None):
    ts = datetime.now().strftime("%H:%M:%S")
    suffix = f"  ({elapsed:.1f}s)" if elapsed is not None else ""
    print(f"[{ts}] {msg}{suffix}")


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\n" + "=" * 55)
    print("  Supply Chain PO Analytics Pipeline")
    print("=" * 55)

    # 1. Generate data
    t0 = time.time()
    _log("Generating synthetic purchase order data...")
    orders_raw, suppliers = data_generator.generate()
    _log(f"  → {len(orders_raw):,} orders, {len(suppliers)} suppliers", time.time() - t0)

    # 2. Run pipeline (clean + enrich + KPIs)
    t0 = time.time()
    _log("Running pipeline & computing KPIs...")
    results = pipeline.run(orders_raw, suppliers)
    _log(f"  → Pipeline complete", time.time() - t0)

    # 3. Risk engine
    t0 = time.time()
    _log("Running at-risk order detection...")
    at_risk = risk_engine.flag_at_risk_orders(results["orders"], results["supplier_kpis"])
    _log(f"  → {len(at_risk)} at-risk open orders flagged", time.time() - t0)

    # 4. Generate Excel report
    t0 = time.time()
    _log("Generating Excel report...")
    report_path = report_generator.generate_report(results, at_risk, timestamp)
    _log(f"  → {report_path}", time.time() - t0)

    # 5. Export CSVs
    t0 = time.time()
    _log("Exporting CSVs...")
    csv_paths = exporter.export_csvs(results, at_risk, timestamp)
    for name, path in csv_paths.items():
        _log(f"  → {path}")
    _log("Done.", time.time() - t0)

    # 6. Print summary table
    exporter.print_summary(results, at_risk)


if __name__ == "__main__":
    main()
