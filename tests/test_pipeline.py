"""Tests for the pipeline cleaning, enrichment, and KPI computation."""

import sys
sys.path.insert(0, "src")

import pandas as pd
import numpy as np
import pytest

import data_generator
import pipeline


@pytest.fixture(scope="module")
def sample_data():
    orders_raw, suppliers = data_generator.generate()
    results = pipeline.run(orders_raw, suppliers)
    return results, suppliers


def test_orders_no_nulls_in_key_columns(sample_data):
    results, _ = sample_data
    orders = results["orders"]
    for col in ("po_id", "supplier_id", "order_date", "status", "product_category"):
        assert orders[col].notna().all(), f"Null found in {col}"


def test_no_duplicate_po_ids(sample_data):
    results, _ = sample_data
    orders = results["orders"]
    assert orders["po_id"].nunique() == len(orders)


def test_delay_cost_non_negative(sample_data):
    results, _ = sample_data
    assert (results["orders"]["delay_cost"].fillna(0) >= 0).all()


def test_days_delayed_non_negative_for_delivered(sample_data):
    results, _ = sample_data
    delivered = results["orders"][results["orders"]["status"] == "Delivered"]
    assert (delivered["days_delayed"].fillna(0) >= 0).all()


def test_otd_rate_in_range(sample_data):
    results, _ = sample_data
    p = results["portfolio_kpis"].iloc[0]
    assert 0.0 <= p["otd_rate"] <= 1.0


def test_supplier_kpis_all_suppliers_present(sample_data):
    results, suppliers = sample_data
    # Not all suppliers may have delivered orders, but no extras should appear
    kpi_sids = set(results["supplier_kpis"]["supplier_id"])
    all_sids = set(suppliers["supplier_id"])
    assert kpi_sids.issubset(all_sids)


def test_reliability_score_in_range(sample_data):
    results, _ = sample_data
    scores = results["supplier_kpis"]["reliability_score"]
    assert (scores >= 0).all()
    assert (scores <= 1).all()


def test_monthly_kpis_row_count(sample_data):
    results, _ = sample_data
    monthly = results["monthly_kpis"]
    # Should have at most 24 months (2023-01 to 2024-12)
    assert len(monthly) <= 24
    assert len(monthly) > 0


def test_total_spend_positive(sample_data):
    results, _ = sample_data
    p = results["portfolio_kpis"].iloc[0]
    assert p["total_spend"] > 0


def test_determinism():
    """Running generate() twice produces identical order sets."""
    orders1, _ = data_generator.generate()
    orders2, _ = data_generator.generate()
    pd.testing.assert_frame_equal(orders1, orders2)
