"""
At-risk open order detection engine.

For each open purchase order, estimates the expected delivery date using
the supplierʼs historical average lead time and applies tiered risk scoring.

Risk levels (severity descending):
  CRITICAL  — supplier is flagged unreliable AND order is already overdue
  HIGH      — supplier flagged unreliable OR order is already overdue
  MEDIUM    — order is within AT_RISK_THRESHOLD_DAYS of its promised date
  LOW       — open order, no immediate concern

Returns a filtered DataFrame of at-risk (MEDIUM+) open orders.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import date
from typing import Optional

import config

_RISK_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}


def _score_risk(row: pd.Series, today: pd.Timestamp) -> tuple[str, str]:
    """Return (risk_level, risk_reason) for a single open order row."""
    elapsed_days = (today - row["order_date"]).days
    expected_delivery = row["order_date"] + pd.Timedelta(days=row["supplier_avg_lead"])
    days_until_promised = (row["promised_delivery_date"] - today).days
    days_past_promised = max(0, (today - row["promised_delivery_date"]).days)
    days_past_expected = max(0, (today - expected_delivery).days)
    is_overdue = days_past_promised > 0
    is_unreliable = row["supplier_flagged_unreliable"]

    if is_overdue and is_unreliable:
        reason = (
            f"Order is {days_past_promised}d past promised date; "
            f"supplier reliability score {row['supplier_reliability']:.0%} "
            f"(below {config.SUPPLIER_RELIABILITY_FLOOR:.0%} threshold)"
        )
        return "CRITICAL", reason

    if is_overdue:
        reason = f"Order is {days_past_promised}d past promised delivery date"
        return "HIGH", reason

    if is_unreliable:
        reason = (
            f"Supplier reliability {row['supplier_reliability']:.0%} "
            f"below threshold; {days_until_promised}d until promised date"
        )
        return "HIGH", reason

    if 0 <= days_until_promised <= config.AT_RISK_THRESHOLD_DAYS:
        reason = f"Promised delivery in {days_until_promised}d; supplier avg lead time is {row['supplier_avg_lead']:.0f}d"
        return "MEDIUM", reason

    return "LOW", "Open order, no immediate risk indicators"


def flag_at_risk_orders(
    orders: pd.DataFrame,
    supplier_kpis: pd.DataFrame,
    today: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Identify and score all open orders.

    Parameters
    ----------
    orders : enriched orders DataFrame from pipeline.run()
    supplier_kpis : supplier KPI DataFrame from pipeline.run()
    today : reference date (defaults to pd.Timestamp.today())

    Returns
    -------
    DataFrame of open orders sorted by risk severity, then PO value descending.
    Includes risk_level, risk_reason, days_past_promised, expected_delivery columns.
    """
    if today is None:
        today = pd.Timestamp.today().normalize()

    open_orders = orders[orders["status"] == "Open"].copy()
    if open_orders.empty:
        return open_orders

    # Join supplier reliability info
    supplier_info = supplier_kpis[
        ["supplier_id", "avg_lead_time", "reliability_score", "flagged_unreliable"]
    ].rename(columns={
        "avg_lead_time": "supplier_avg_lead",
        "reliability_score": "supplier_reliability",
        "flagged_unreliable": "supplier_flagged_unreliable",
    })

    # For suppliers with no delivered history yet, use tier default
    tier_defaults = {
        t: v["base_lead_days"] for t, v in config.SUPPLIER_TIERS.items()
    }
    open_orders = open_orders.merge(supplier_info, on="supplier_id", how="left")

    # Fill missing avg lead time from tier
    mask = open_orders["supplier_avg_lead"].isna()
    if mask.any():
        open_orders.loc[mask, "supplier_avg_lead"] = open_orders.loc[mask, "tier"].map(tier_defaults)
    open_orders["supplier_avg_lead"] = open_orders["supplier_avg_lead"].fillna(14)
    open_orders["supplier_reliability"] = open_orders["supplier_reliability"].fillna(1.0)
    open_orders["supplier_flagged_unreliable"] = open_orders["supplier_flagged_unreliable"].fillna(False)

    # Score each order
    scores = open_orders.apply(lambda r: _score_risk(r, today), axis=1)
    open_orders["risk_level"] = scores.apply(lambda x: x[0])
    open_orders["risk_reason"] = scores.apply(lambda x: x[1])

    # Derived columns useful for the report
    open_orders["days_since_order"] = (today - open_orders["order_date"]).dt.days
    open_orders["days_past_promised"] = (
        (today - open_orders["promised_delivery_date"]).dt.days.clip(lower=0)
    )
    open_orders["expected_delivery"] = open_orders["order_date"] + pd.to_timedelta(
        open_orders["supplier_avg_lead"], unit="D"
    )

    # Filter to MEDIUM and above for the report
    at_risk = open_orders[open_orders["risk_level"].isin(["MEDIUM", "HIGH", "CRITICAL"])].copy()

    # Sort by severity descending, then by PO value descending
    at_risk["_sev"] = at_risk["risk_level"].map(_RISK_ORDER)
    at_risk = at_risk.sort_values(["_sev", "total_po_value"], ascending=[False, False])
    at_risk = at_risk.drop(columns=["_sev"])

    return at_risk.reset_index(drop=True)
