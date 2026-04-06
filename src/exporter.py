"""
CSV export and CLI summary output.

Writes three CSV files to data/output/ and prints a brief summary table
to stdout so the pipeline gives immediate feedback when run from terminal.
"""

import os
from datetime import datetime

import pandas as pd
from tabulate import tabulate

import config


def _ensure_output_dir():
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)


def export_csvs(
    pipeline_results: dict,
    at_risk: pd.DataFrame,
    timestamp: str,
) -> dict[str, str]:
    """
    Write enriched orders, KPI summary, and at-risk orders to CSV.
    Returns a dict mapping name -> file path.
    """
    _ensure_output_dir()

    paths = {}

    orders_path = os.path.join(config.OUTPUT_DIR, f"orders_enriched_{timestamp}.csv")
    pipeline_results["orders"].to_csv(orders_path, index=False)
    paths["orders_enriched"] = orders_path

    kpi_path = os.path.join(config.OUTPUT_DIR, f"kpi_summary_{timestamp}.csv")
    pipeline_results["supplier_kpis"].to_csv(kpi_path, index=False)
    paths["kpi_summary"] = kpi_path

    risk_path = os.path.join(config.OUTPUT_DIR, f"at_risk_orders_{timestamp}.csv")
    at_risk.to_csv(risk_path, index=False)
    paths["at_risk_orders"] = risk_path

    return paths


def print_summary(pipeline_results: dict, at_risk: pd.DataFrame):
    """Print a concise portfolio summary to stdout."""
    p = pipeline_results["portfolio_kpis"].iloc[0]

    summary_rows = [
        ["Total Orders",          f"{p['total_orders']:,}"],
        ["Delivered",             f"{p['delivered_orders']:,}"],
        ["Open",                  f"{p['open_orders']:,}"],
        ["Cancelled",             f"{p['cancelled_orders']:,}"],
        ["On-Time Delivery Rate", f"{p['otd_rate']:.1%}"],
        ["Avg Lead Time",         f"{p['avg_lead_time_days']:.1f} days"],
        ["Avg Delay (when late)", f"{p['avg_delay_when_late_days']:.1f} days"],
        ["Total Spend",           f"${p['total_spend']:,.0f}"],
        ["Total Delay Cost",      f"${p['total_delay_cost']:,.0f}"],
        ["Delay Cost % of Spend", f"{p['delay_cost_pct_spend']:.2%}"],
        ["Unreliable Suppliers",  f"{p['unreliable_suppliers']}"],
        ["At-Risk Open Orders",   f"{len(at_risk)}"],
    ]

    print("\n" + "=" * 50)
    print("  SUPPLY CHAIN PORTFOLIO SUMMARY")
    print("=" * 50)
    print(tabulate(summary_rows, tablefmt="simple"))

    if not at_risk.empty:
        top_risk = at_risk[["po_id", "supplier_name", "risk_level", "total_po_value"]].head(5)
        top_risk = top_risk.copy()
        top_risk["total_po_value"] = top_risk["total_po_value"].apply(lambda x: f"${x:,.0f}")
        print("\nTop At-Risk Orders:")
        print(tabulate(top_risk, headers="keys", tablefmt="simple", showindex=False))

    print("=" * 50 + "\n")
