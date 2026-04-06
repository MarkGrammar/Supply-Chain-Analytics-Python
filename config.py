"""
Central configuration for the Supply Chain PO Analytics Pipeline.
Modify these values to adjust simulation parameters and business thresholds.
"""

# ── Simulation parameters ────────────────────────────────────────────────────
RANDOM_SEED = 42
NUM_ORDERS = 1500
NUM_SUPPLIERS = 25
SIMULATION_START_DATE = "2023-01-01"
SIMULATION_END_DATE = "2024-12-31"

# ── Supplier tier definitions ─────────────────────────────────────────────────
# base_lead_days : median lead time for a healthy order
# variance       : std dev in days (higher = less predictable)
# delay_prob     : probability any given order is delayed
SUPPLIER_TIERS = {
    "Tier 1": {"base_lead_days": 7,  "variance": 2, "delay_prob": 0.05},
    "Tier 2": {"base_lead_days": 14, "variance": 5, "delay_prob": 0.15},
    "Tier 3": {"base_lead_days": 21, "variance": 8, "delay_prob": 0.30},
}

# ── Business thresholds ───────────────────────────────────────────────────────
# Cost penalty accrued per day an order is late, as a fraction of PO value
DELAY_COST_RATE_PER_DAY = 0.002          # 0.2% of PO value per day

# Open orders expected >= this many days late are flagged as at-risk
AT_RISK_THRESHOLD_DAYS = 3

# Suppliers with reliability score below this are flagged as unreliable
SUPPLIER_RELIABILITY_FLOOR = 0.75

# Weights for the composite reliability score
RELIABILITY_WEIGHT_OTD = 0.60            # On-time delivery rate weight
RELIABILITY_WEIGHT_CONSISTENCY = 0.40   # Lead-time consistency (1 - CV) weight

# ── Product categories ────────────────────────────────────────────────────────
PRODUCT_CATEGORIES = [
    "Electronics",
    "Raw Materials",
    "Packaging",
    "Logistics Services",
    "MRO",
]

# ── Regions ───────────────────────────────────────────────────────────────────
REGIONS = ["North America", "EMEA", "APAC"]

# ── Payment terms (days) ──────────────────────────────────────────────────────
PAYMENT_TERMS_OPTIONS = [30, 45, 60]

# ── Output paths ─────────────────────────────────────────────────────────────
OUTPUT_DIR = "data/output"
