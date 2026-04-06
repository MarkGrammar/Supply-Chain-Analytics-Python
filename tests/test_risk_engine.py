"""Tests for the risk engine flagging logic."""

import sys
sys.path.insert(0, "src")

import pandas as pd
import numpy as np
import pytest

import data_generator
import pipeline
import risk_engine


@pytest.fixture(scope="module")
def pipeline_results():
    orders_raw, suppliers = data_generator.generate()
    return pipeline.run(orders_raw, suppliers)


def test_at_risk_returns_dataframe(pipeline_results):
    at_risk = risk_engine.flag_at_risk_orders(
        pipeline_results["orders"], pipeline_results["supplier_kpis"]
    )
    assert isinstance(at_risk, pd.DataFrame)


def test_at_risk_only_open_orders(pipeline_results):
    at_risk = risk_engine.flag_at_risk_orders(
        pipeline_results["orders"], pipeline_results["supplier_kpis"]
    )
    if not at_risk.empty:
        assert (at_risk["status"] == "Open").all()


def test_risk_levels_valid(pipeline_results):
    at_risk = risk_engine.flag_at_risk_orders(
        pipeline_results["orders"], pipeline_results["supplier_kpis"]
    )
    valid_levels = {"CRITICAL", "HIGH", "MEDIUM"}
    if not at_risk.empty:
        assert set(at_risk["risk_level"].unique()).issubset(valid_levels)


def test_critical_requires_overdue_and_unreliable():
    """CRITICAL orders must be both overdue and from unreliable suppliers."""
    today = pd.Timestamp("2024-06-15")

    orders = pd.DataFrame([{
        "po_id": "PO-TEST-001",
        "supplier_id": "SUP-001",
        "order_date": pd.Timestamp("2024-05-01"),
        "promised_delivery_date": pd.Timestamp("2024-05-20"),  # overdue
        "actual_delivery_date": None,
        "status": "Open",
        "product_category": "Electronics",
        "quantity": 100,
        "unit_cost": 50.0,
        "total_po_value": 5000.0,
        "buyer_region": "EMEA",
        "payment_terms_days": 30,
        "tier": "Tier 3",
        "supplier_name": "Test Supplier",
        "supplier_region": "EMEA",
        "promised_lead_time": 19,
        "actual_lead_time": None,
        "days_delayed": None,
        "is_delayed": False,
        "is_on_time": False,
        "delay_cost": 0,
        "order_month": pd.Period("2024-05", "M"),
    }])

    supplier_kpis = pd.DataFrame([{
        "supplier_id": "SUP-001",
        "supplier_name": "Test Supplier",
        "tier": "Tier 3",
        "avg_lead_time": 25.0,
        "reliability_score": 0.60,  # below floor
        "flagged_unreliable": True,
    }])

    at_risk = risk_engine.flag_at_risk_orders(orders, supplier_kpis, today=today)
    assert len(at_risk) == 1
    assert at_risk.iloc[0]["risk_level"] == "CRITICAL"


def test_no_open_orders_returns_empty():
    """If no open orders exist, result should be empty DataFrame."""
    orders_raw, suppliers = data_generator.generate()
    results = pipeline.run(orders_raw, suppliers)

    closed_orders = results["orders"][results["orders"]["status"] != "Open"].copy()
    at_risk = risk_engine.flag_at_risk_orders(closed_orders, results["supplier_kpis"])
    assert at_risk.empty


def test_sorted_by_severity(pipeline_results):
    """At-risk orders should be sorted CRITICAL > HIGH > MEDIUM."""
    at_risk = risk_engine.flag_at_risk_orders(
        pipeline_results["orders"], pipeline_results["supplier_kpis"]
    )
    if len(at_risk) < 2:
        return
    order_map = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    scores = at_risk["risk_level"].map(order_map).tolist()
    assert scores == sorted(scores, reverse=True)
