"""
Data cleaning, enrichment, and KPI computation layer.

Takes raw DataFrames from data_generator and returns a dict of clean,
analysis-ready DataFrames:
  {
    "orders":          enriched order-level DataFrame,
    "supplier_kpis":   per-supplier KPI summary,
    "monthly_kpis":    monthly time-series aggregation,
    "portfolio_kpis":  single-row overall summary,
  }
"""

import pandas as pd
import numpy as np

import config


# ── Cleaning ──────────────────────────────────────────────────────────────────

def clean_orders(orders: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    df = orders.copy()

    # Enforce dtypes
    for col in ("order_date", "promised_delivery_date", "actual_delivery_date"):
        df[col] = pd.to_datetime(df[col])

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce")

    # Drop duplicate PO IDs (keep first)
    df = df.drop_duplicates(subset="po_id", keep="first")

    # Referential integrity: keep only orders whose supplier exists
    valid_sids = set(suppliers["supplier_id"])
    df = df[df["supplier_id"].isin(valid_sids)].copy()

    # Impute missing unit_cost with per-category median
    category_medians = df.groupby("product_category")["unit_cost"].transform("median")
    df["unit_cost"] = df["unit_cost"].fillna(category_medians)

    # Recalculate total_po_value after imputation
    df["total_po_value"] = (df["quantity"].astype(float) * df["unit_cost"]).round(2)

    return df


# ── Enrichment ────────────────────────────────────────────────────────────────

def enrich_orders(orders: pd.DataFrame, suppliers: pd.DataFrame) -> pd.DataFrame:
    df = orders.merge(
        suppliers[["supplier_id", "supplier_name", "tier", "region"]].rename(
            columns={"region": "supplier_region"}
        ),
        on="supplier_id",
        how="left",
    )

    df["promised_lead_time"] = (
        df["promised_delivery_date"] - df["order_date"]
    ).dt.days

    df["actual_lead_time"] = (
        df["actual_delivery_date"] - df["order_date"]
    ).dt.days

    delivered = df["status"] == "Delivered"
    df["days_delayed"] = np.where(
        delivered,
        (df["actual_delivery_date"] - df["promised_delivery_date"]).dt.days.clip(lower=0),
        np.nan,
    )

    df["is_delayed"] = delivered & (df["days_delayed"] > 0)
    df["is_on_time"] = delivered & (df["days_delayed"] == 0)

    df["delay_cost"] = (
        df["days_delayed"].fillna(0)
        * df["total_po_value"]
        * config.DELAY_COST_RATE_PER_DAY
    ).round(2)

    df["order_month"] = df["order_date"].dt.to_period("M")

    return df


# ── KPI computation ───────────────────────────────────────────────────────────

def compute_supplier_kpis(orders: pd.DataFrame) -> pd.DataFrame:
    delivered = orders[orders["status"] == "Delivered"]

    grp = delivered.groupby(["supplier_id", "supplier_name", "tier"])

    kpis = grp.agg(
        order_count=("po_id", "count"),
        total_spend=("total_po_value", "sum"),
        total_delay_cost=("delay_cost", "sum"),
        otd_count=("is_on_time", "sum"),
        avg_lead_time=("actual_lead_time", "mean"),
        std_lead_time=("actual_lead_time", "std"),
    ).reset_index()

    kpis["otd_rate"] = (kpis["otd_count"] / kpis["order_count"]).round(4)
    kpis["avg_lead_time"] = kpis["avg_lead_time"].round(1)
    kpis["std_lead_time"] = kpis["std_lead_time"].fillna(0).round(1)

    # Coefficient of variation: std / mean (0 = perfectly consistent)
    kpis["lead_time_cv"] = (
        kpis["std_lead_time"] / kpis["avg_lead_time"].replace(0, np.nan)
    ).fillna(0).clip(upper=1.0).round(4)

    # Composite reliability score
    kpis["reliability_score"] = (
        config.RELIABILITY_WEIGHT_OTD * kpis["otd_rate"]
        + config.RELIABILITY_WEIGHT_CONSISTENCY * (1 - kpis["lead_time_cv"])
    ).round(4)

    kpis["delay_cost_pct_spend"] = (
        kpis["total_delay_cost"] / kpis["total_spend"].replace(0, np.nan)
    ).fillna(0).round(4)

    kpis["flagged_unreliable"] = kpis["reliability_score"] < config.SUPPLIER_RELIABILITY_FLOOR

    kpis["total_spend"] = kpis["total_spend"].round(2)
    kpis["total_delay_cost"] = kpis["total_delay_cost"].round(2)

    return kpis.sort_values("reliability_score")


def compute_monthly_kpis(orders: pd.DataFrame) -> pd.DataFrame:
    grp = orders.groupby("order_month")

    delivered = orders[orders["status"] == "Delivered"]
    d_grp = delivered.groupby("order_month")

    monthly = grp.agg(
        po_count=("po_id", "count"),
        total_spend=("total_po_value", "sum"),
        total_delay_cost=("delay_cost", "sum"),
    )

    otd = d_grp.agg(
        delivered_count=("po_id", "count"),
        on_time_count=("is_on_time", "sum"),
    )

    monthly = monthly.join(otd, how="left")
    monthly["otd_rate"] = (
        monthly["on_time_count"] / monthly["delivered_count"]
    ).fillna(0).round(4)

    monthly["total_spend"] = monthly["total_spend"].round(2)
    monthly["total_delay_cost"] = monthly["total_delay_cost"].round(2)

    return monthly.reset_index()


def compute_portfolio_kpis(orders: pd.DataFrame, supplier_kpis: pd.DataFrame) -> pd.DataFrame:
    delivered = orders[orders["status"] == "Delivered"]
    total = len(orders)

    otd_rate = (delivered["is_on_time"].sum() / len(delivered)) if len(delivered) else 0
    avg_lead = delivered["actual_lead_time"].mean()
    avg_delay = delivered.loc[delivered["is_delayed"], "days_delayed"].mean()
    total_spend = orders["total_po_value"].sum()
    total_delay_cost = orders["delay_cost"].sum()
    open_count = (orders["status"] == "Open").sum()
    cancelled_count = (orders["status"] == "Cancelled").sum()
    unreliable_count = supplier_kpis["flagged_unreliable"].sum()

    return pd.DataFrame([{
        "total_orders": total,
        "delivered_orders": len(delivered),
        "open_orders": int(open_count),
        "cancelled_orders": int(cancelled_count),
        "otd_rate": round(otd_rate, 4),
        "avg_lead_time_days": round(avg_lead, 1),
        "avg_delay_when_late_days": round(avg_delay, 1) if not pd.isna(avg_delay) else 0,
        "total_spend": round(total_spend, 2),
        "total_delay_cost": round(total_delay_cost, 2),
        "delay_cost_pct_spend": round(total_delay_cost / total_spend, 4) if total_spend else 0,
        "cancellation_rate": round(cancelled_count / total, 4) if total else 0,
        "unreliable_suppliers": int(unreliable_count),
    }])


# ── Public entry point ────────────────────────────────────────────────────────

def run(orders_raw: pd.DataFrame, suppliers: pd.DataFrame) -> dict:
    """
    Clean, enrich, and compute KPIs.
    Returns a dict of DataFrames keyed by name.
    """
    orders_clean = clean_orders(orders_raw, suppliers)
    orders_enriched = enrich_orders(orders_clean, suppliers)
    supplier_kpis = compute_supplier_kpis(orders_enriched)
    monthly_kpis = compute_monthly_kpis(orders_enriched)
    portfolio_kpis = compute_portfolio_kpis(orders_enriched, supplier_kpis)

    return {
        "orders": orders_enriched,
        "supplier_kpis": supplier_kpis,
        "monthly_kpis": monthly_kpis,
        "portfolio_kpis": portfolio_kpis,
    }
