"""Configuration for the longitudinal data prototype.

Everything in this file is a knob. Change values here and re-run
`scripts/bootstrap.sh` to regenerate data and layers with a different scale.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
LAYER1_DIR = DATA_DIR / "layer1_sessions"
LAYER2_DIR = DATA_DIR / "layer2_daily"
LAYER3_DIR = DATA_DIR / "layer3_decomposed"
LAYER4_DIR = DATA_DIR / "layer4_ai_ready"

for d in (RAW_DIR, LAYER1_DIR, LAYER2_DIR, LAYER3_DIR, LAYER4_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Scale knobs
# ---------------------------------------------------------------------------

NUM_USERS = 1000
START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 6, 30)  # 18 months

# Random seed for reproducibility
SEED = 42

# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

MARKETS = ["US", "IN", "BR", "DE", "JP"]
MARKET_WEIGHTS = [0.35, 0.28, 0.14, 0.13, 0.10]  # traffic share

CATEGORIES = ["Games", "Productivity", "Social", "Shopping", "Entertainment"]
CATEGORY_WEIGHTS = [0.35, 0.15, 0.20, 0.15, 0.15]

SEGMENTS = ["new", "casual", "power", "churning"]
SEGMENT_WEIGHTS = [0.20, 0.50, 0.20, 0.10]

# ---------------------------------------------------------------------------
# Event vocabulary
# ---------------------------------------------------------------------------

EVENT_TYPES = [
    "session_start",
    "search",
    "browse_category",
    "app_view",
    "app_install",
    "app_launch",
    "purchase",
    "offer_impression",
    "offer_click",
    "offer_redeem",
    "rating_submit",
    "session_end",
]

# ---------------------------------------------------------------------------
# Persona definitions: per-day session rate (lambda for Poisson) and
# event-type mix within a session. Weights don't need to sum to 1 — they're
# normalized at runtime.
# ---------------------------------------------------------------------------

PERSONAS: dict[str, dict] = {
    "casual_browser": {
        "share": 0.30,
        "sessions_per_day": 0.25,
        "events_per_session_mean": 6,
        "mix": {
            "session_start": 1.0,
            "browse_category": 3.0,
            "app_view": 2.0,
            "search": 0.5,
            "session_end": 1.0,
        },
        "conversion_p": 0.01,
        "offer_affinity": 0.3,
    },
    "deal_hunter": {
        "share": 0.15,
        "sessions_per_day": 0.7,
        "events_per_session_mean": 9,
        "mix": {
            "session_start": 1.0,
            "search": 1.5,
            "offer_impression": 2.0,
            "offer_click": 1.5,
            "offer_redeem": 1.0,
            "app_view": 1.0,
            "session_end": 1.0,
        },
        "conversion_p": 0.08,
        "offer_affinity": 1.5,
    },
    "committed_buyer": {
        "share": 0.10,
        "sessions_per_day": 0.35,
        "events_per_session_mean": 7,
        "mix": {
            "session_start": 1.0,
            "search": 1.0,
            "app_view": 1.5,
            "app_install": 0.5,
            "purchase": 0.4,
            "app_launch": 1.0,
            "session_end": 1.0,
        },
        "conversion_p": 0.22,
        "offer_affinity": 0.5,
    },
    "power_user": {
        "share": 0.08,
        "sessions_per_day": 1.2,
        "events_per_session_mean": 12,
        "mix": {
            "session_start": 1.0,
            "browse_category": 2.0,
            "app_view": 3.0,
            "app_launch": 2.0,
            "search": 1.0,
            "rating_submit": 0.3,
            "purchase": 0.2,
            "session_end": 1.0,
        },
        "conversion_p": 0.12,
        "offer_affinity": 0.7,
    },
    "new_user": {
        "share": 0.15,
        "sessions_per_day": 0.5,
        "events_per_session_mean": 8,
        "mix": {
            "session_start": 1.0,
            "browse_category": 2.5,
            "app_view": 2.0,
            "app_install": 0.8,
            "app_launch": 0.8,
            "session_end": 1.0,
        },
        "conversion_p": 0.04,
        "offer_affinity": 0.8,
    },
    "churning": {
        "share": 0.12,
        "sessions_per_day": 0.1,
        "events_per_session_mean": 4,
        "mix": {
            "session_start": 1.0,
            "app_launch": 2.0,
            "session_end": 1.0,
        },
        "conversion_p": 0.005,
        "offer_affinity": 0.2,
    },
    "bot_like": {
        "share": 0.05,
        "sessions_per_day": 2.5,
        "events_per_session_mean": 3,
        "mix": {
            "session_start": 1.0,
            "search": 4.0,
            "session_end": 1.0,
        },
        "conversion_p": 0.0,
        "offer_affinity": 0.0,
    },
    "returner": {
        "share": 0.05,
        "sessions_per_day": 0.3,
        "events_per_session_mean": 7,
        "mix": {
            "session_start": 1.0,
            "browse_category": 2.0,
            "app_view": 2.0,
            "app_launch": 1.0,
            "session_end": 1.0,
        },
        "conversion_p": 0.06,
        "offer_affinity": 0.6,
    },
}

# ---------------------------------------------------------------------------
# Initiative calendar — multiplicative perturbations on event rates.
# These are the ground-truth "causes" the agent gets to attribute.
# ---------------------------------------------------------------------------

INITIATIVES: list[dict] = [
    {
        "id": "diwali_2024",
        "name": "Diwali Sale India",
        "market": "IN",
        "category": "Games",
        "start": date(2024, 10, 20),
        "end": date(2024, 11, 5),
        "effects": {"offer_redeem": 2.1, "offer_click": 1.5, "purchase": 1.3},
    },
    {
        "id": "black_friday_2024",
        "name": "Black Friday Global",
        "market": None,  # global
        "category": "Shopping",
        "start": date(2024, 11, 28),
        "end": date(2024, 12, 2),
        "effects": {"purchase": 1.8, "offer_click": 1.6, "offer_redeem": 1.7},
    },
    {
        "id": "play_campaign_q1",
        "name": "Play Store Spring Campaign",
        "market": None,
        "category": None,
        "start": date(2025, 3, 10),
        "end": date(2025, 3, 25),
        "effects": {"offer_impression": 1.4, "offer_click": 1.3},
    },
    {
        "id": "anti_fraud_push",
        "name": "Anti-fraud push",
        "market": None,
        "category": None,
        "start": date(2025, 4, 1),
        "end": date(2025, 12, 31),  # ongoing
        "effects": {"bot_session_rate_multiplier": 0.25},  # special key
    },
    {
        "id": "new_recommender",
        "name": "New Recommender Model Rollout",
        "market": None,
        "category": None,
        "start": date(2025, 2, 15),
        "end": date(2025, 12, 31),
        "effects": {"app_install": 1.15, "app_view": 1.08},
    },
    {
        "id": "us_summer_sale",
        "name": "US Summer Games Sale",
        "market": "US",
        "category": "Games",
        "start": date(2025, 6, 15),
        "end": date(2025, 6, 28),
        "effects": {"offer_redeem": 1.7, "purchase": 1.4},
    },
]

# ---------------------------------------------------------------------------
# Seasonal modulation — month-of-year multipliers per metric.
# Keep gentle; the big movements come from initiatives.
# ---------------------------------------------------------------------------

SEASONAL_MONTH_MULT: dict[str, list[float]] = {
    # Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec
    "purchase":     [0.9, 0.85, 0.95, 1.0, 1.0, 1.05, 1.1, 1.05, 1.0, 1.0, 1.15, 1.25],
    "offer_redeem": [0.95, 0.9, 1.0, 1.0, 1.05, 1.05, 1.1, 1.05, 1.0, 1.1, 1.2, 1.2],
    "app_install":  [1.05, 1.0, 1.0, 0.95, 0.95, 1.0, 1.05, 1.05, 1.0, 1.0, 1.05, 1.1],
}

# ---------------------------------------------------------------------------
# Pipeline parameters
# ---------------------------------------------------------------------------

# Change-point detection
CHANGE_POINT_Z_THRESHOLD = 2.5
CHANGE_POINT_MIN_MAGNITUDE_PCT = 0.08  # 8%

# Journey archetypes
NUM_ARCHETYPES = 20  # smaller for prototype clarity

# Behavioral embeddings
EMBEDDING_DIM = 8

# STL
STL_WEEKLY_PERIOD = 13  # quarterly seasonality (52 would need ≥104 weeks)
