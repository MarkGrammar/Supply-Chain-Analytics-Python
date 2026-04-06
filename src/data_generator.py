"""
Generates deterministic synthetic purchase order and supplier data.
All randomness is seeded via config.RANDOM_SEED so every run produces
the same dataset — important for portfolio reproducibility.
"""

import numpy as np
import pandas as pd
from faker import Faker
from datetime import timedelta

import config

fake = Faker()
rng = np.random.default_rng(config.RANDOM_SEED)  # reset in generate()
Faker.seed(config.RANDOM_SEED)


# ── Supplier generation ───────────────────────────────────────────────────────

def generate_suppliers() -> pd.DataFrame:
    """Return a DataFrame of NUM_SUPPLIERS realistic supplier records."""
    tiers = list(config.SUPPLIER_TIERS.keys())
    # Distribute suppliers roughly evenly across tiers, with fewer Tier 1
    tier_counts = [
        config.NUM_SUPPLIERS // 5,                          # Tier 1 (fewest)
        config.NUM_SUPPLIERS // 2,                          # Tier 2 (most)
        config.NUM_SUPPLIERS - config.NUM_SUPPLIERS // 5 - config.NUM_SUPPLIERS // 2,  # Tier 3
    ]

    records = []
    sid = 1
    for tier, count in zip(tiers, tier_counts):
        for _ in range(count):
            records.append({
                "supplier_id": f"SUP-{sid:03d}",
                "supplier_name": fake.company(),
                "tier": tier,
                "region": rng.choice(config.REGIONS),
                "category_specialization": rng.choice(config.PRODUCT_CATEGORIES),
                "contract_start_date": fake.date_between(
                    start_date="-5y", end_date="-1y"
                ),
                "payment_terms_days": int(rng.choice(config.PAYMENT_TERMS_OPTIONS)),
            })
            sid += 1

    return pd.DataFrame(records)


# ── Order generation ──────────────────────────────────────────────────────────

def _make_order(po_num: int, suppliers: pd.DataFrame) -> dict:
    """Generate a single purchase order record."""
    supplier = suppliers.sample(1, random_state=int(rng.integers(0, 2**31))).iloc[0]
    tier_cfg = config.SUPPLIER_TIERS[supplier["tier"]]

    order_date = pd.Timestamp(config.SIMULATION_START_DATE) + timedelta(
        days=int(rng.integers(
            0,
            (pd.Timestamp(config.SIMULATION_END_DATE) - pd.Timestamp(config.SIMULATION_START_DATE)).days - 30,
        ))
    )

    promised_lead = max(3, int(rng.normal(
        loc=tier_cfg["base_lead_days"],
        scale=tier_cfg["variance"],
    )))
    promised_delivery = order_date + timedelta(days=promised_lead)

    # Decide status: orders promised after ~6 months ago are likely still open
    cutoff = pd.Timestamp("today") - timedelta(days=180)
    if promised_delivery > pd.Timestamp(config.SIMULATION_END_DATE):
        status = "Open"
    elif rng.random() < 0.04:
        status = "Cancelled"
    else:
        status = "Delivered"

    actual_delivery = None
    days_late_extra = 0
    if status == "Delivered":
        is_delayed = rng.random() < tier_cfg["delay_prob"]
        if is_delayed:
            days_late_extra = int(rng.integers(1, 15))
        actual_delivery = promised_delivery + timedelta(days=days_late_extra)

    quantity = int(rng.integers(10, 500))
    unit_cost = round(float(rng.uniform(5.0, 500.0)), 2)
    # Occasionally introduce a missing unit_cost (realistic dirty data)
    if rng.random() < 0.015:
        unit_cost = None

    category = supplier["category_specialization"] if rng.random() < 0.6 else rng.choice(config.PRODUCT_CATEGORIES)

    return {
        "po_id": f"PO-{order_date.year}-{po_num:06d}",
        "supplier_id": supplier["supplier_id"],
        "order_date": order_date.date(),
        "promised_delivery_date": promised_delivery.date(),
        "actual_delivery_date": actual_delivery.date() if actual_delivery else None,
        "status": status,
        "product_category": category,
        "quantity": quantity,
        "unit_cost": unit_cost,
        "total_po_value": round(quantity * unit_cost, 2) if unit_cost else None,
        "buyer_region": rng.choice(config.REGIONS),
        "payment_terms_days": int(supplier["payment_terms_days"]),
    }


def generate_orders(suppliers: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame of NUM_ORDERS purchase order records."""
    records = [_make_order(i + 1, suppliers) for i in range(config.NUM_ORDERS)]
    df = pd.DataFrame(records)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["promised_delivery_date"] = pd.to_datetime(df["promised_delivery_date"])
    df["actual_delivery_date"] = pd.to_datetime(df["actual_delivery_date"])
    return df


# ── Public entry point ────────────────────────────────────────────────────────

def generate() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate and return (orders_df, suppliers_df).
    Both DataFrames are raw — pipeline.py handles cleaning.
    Re-seeds all generators so the output is identical on every call.
    """
    global rng
    rng = np.random.default_rng(config.RANDOM_SEED)
    Faker.seed(config.RANDOM_SEED)
    fake.seed_instance(config.RANDOM_SEED)

    suppliers = generate_suppliers()
    orders = generate_orders(suppliers)
    return orders, suppliers
