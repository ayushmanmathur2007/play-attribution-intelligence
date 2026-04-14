"""
Seasonal multiplier computation for Google Play Loyalty & Offers metrics.

Produces multipliers centered around 1.0 that represent day-of-week effects,
monthly patterns, market-specific cultural events, and year-end spikes.
All transitions use cosine ramps to avoid sharp discontinuities.
"""

from __future__ import annotations

import math
from datetime import datetime, date, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# Helper: cosine-ramped event multiplier
# ---------------------------------------------------------------------------

def _smooth_event(
    dt: date,
    event_start: date,
    event_end: date,
    peak_multiplier: float,
    ramp_days: int = 3,
) -> float:
    """Return a multiplier with smooth cosine ramp-up / ramp-down.

    Outside the event window (including ramp) the multiplier is 1.0.
    Inside the core window it equals *peak_multiplier*.
    During the ramp periods it follows a raised-cosine curve between
    1.0 and *peak_multiplier*.
    """
    ramp_start = event_start - timedelta(days=ramp_days)
    ramp_end = event_end + timedelta(days=ramp_days)

    if dt < ramp_start or dt > ramp_end:
        return 1.0

    effect = peak_multiplier - 1.0  # the "delta" above baseline

    # Ramp-up phase
    if dt < event_start:
        days_into_ramp = (dt - ramp_start).days
        t = days_into_ramp / ramp_days  # 0 .. 1
        # cosine ramp: 0 at t=0, 1 at t=1
        return 1.0 + effect * 0.5 * (1.0 - math.cos(math.pi * t))

    # Ramp-down phase
    if dt > event_end:
        days_past_end = (dt - event_end).days
        t = days_past_end / ramp_days  # 0 .. 1
        return 1.0 + effect * 0.5 * (1.0 + math.cos(math.pi * t))

    # Core event window
    return peak_multiplier


# ---------------------------------------------------------------------------
# Category helpers
# ---------------------------------------------------------------------------

def _is_gaming(category_id: str) -> bool:
    return category_id.upper().startswith("GAM_")


def _is_productivity(category_id: str) -> bool:
    cat = category_id.upper()
    return cat.startswith("PROD_") or cat.startswith("PRODUCTIVITY")


def _is_finance(category_id: str) -> bool:
    cat = category_id.upper()
    return cat.startswith("FIN_") or cat.startswith("FINANCE")


def _is_shopping(category_id: str) -> bool:
    cat = category_id.upper()
    return cat.startswith("SHOP_") or cat.startswith("COMMERCE")


def _is_entertainment(category_id: str) -> bool:
    cat = category_id.upper()
    return cat.startswith("ENT_") or cat.startswith("ENTERTAINMENT")


def _is_revenue_metric(metric_name: str) -> bool:
    m = metric_name.lower()
    return any(kw in m for kw in ("revenue", "purchase", "spend", "arpu", "arppu"))


def _is_offer_loyalty_metric(metric_name: str) -> bool:
    m = metric_name.lower()
    return any(kw in m for kw in ("offer", "loyalty", "reward", "redeem", "coupon", "points"))


# ---------------------------------------------------------------------------
# 1. Day-of-week multiplier
# ---------------------------------------------------------------------------

def _day_of_week_multiplier(dt: date, category_id: str) -> float:
    weekday = dt.weekday()  # 0=Mon .. 6=Sun
    is_weekend = weekday >= 5

    if is_weekend:
        if _is_gaming(category_id):
            return 1.15
        if _is_productivity(category_id) or _is_finance(category_id):
            return 0.90
        return 1.0

    # Weekday
    if _is_productivity(category_id):
        return 1.05
    return 1.0


# ---------------------------------------------------------------------------
# 2. Monthly pattern multiplier (cosine-smoothed around month boundaries)
# ---------------------------------------------------------------------------

def _monthly_multiplier(dt: date, category_id: str, metric_name: str) -> float:
    month = dt.month
    day = dt.day

    # We smooth transitions around month boundaries using the day-of-month
    # position.  For simplicity the core effect sits in the middle of the
    # month and tapers at the edges via a small helper.

    def _month_blend(target_month: int, base: float) -> float:
        """Return *base* when solidly inside *target_month*, blending toward
        1.0 in the first/last 5 days to avoid hard steps."""
        if month != target_month:
            return 1.0
        days_in = day
        days_left = 31 - day  # approximate
        ramp = 5
        if days_in <= ramp:
            t = days_in / ramp
            return 1.0 + (base - 1.0) * 0.5 * (1.0 - math.cos(math.pi * t))
        if days_left <= ramp:
            t = days_left / ramp
            return 1.0 + (base - 1.0) * 0.5 * (1.0 - math.cos(math.pi * t))
        return base

    multiplier = 1.0

    # January: post-holiday dip for revenue/purchase metrics
    if _is_revenue_metric(metric_name):
        multiplier *= _month_blend(1, 0.90)

    # March: end-of-Q1 push
    multiplier *= _month_blend(3, 1.05)

    # June-August: summer
    if month in (6, 7, 8):
        if _is_gaming(category_id):
            summer_base = 1.10
        elif _is_productivity(category_id):
            summer_base = 0.95
        else:
            summer_base = 1.0
        # Smooth entry in June, smooth exit in August
        if month == 6:
            t = day / 30.0
            summer_effect = 0.5 * (1.0 - math.cos(math.pi * t))
            multiplier *= 1.0 + (summer_base - 1.0) * summer_effect
        elif month == 8:
            t = (31 - day) / 31.0
            summer_effect = 0.5 * (1.0 - math.cos(math.pi * t))
            multiplier *= 1.0 + (summer_base - 1.0) * summer_effect
        else:
            multiplier *= summer_base

    # November: pre-holiday ramp for shopping/commerce
    if _is_shopping(category_id):
        multiplier *= _month_blend(11, 1.15)

    # December: holiday peak
    if month == 12:
        if _is_gaming(category_id):
            multiplier *= _month_blend(12, 1.25)
        elif _is_shopping(category_id):
            multiplier *= _month_blend(12, 1.30)
        else:
            multiplier *= _month_blend(12, 1.10)

    return multiplier


# ---------------------------------------------------------------------------
# 3. Market-specific events
# ---------------------------------------------------------------------------

def _market_event_multiplier(
    dt: date,
    market_id: str,
    category_id: str,
    metric_name: str,
) -> float:
    """Combine all market-specific cultural/commercial event effects."""
    year = dt.year
    mkt = market_id.upper()
    multiplier = 1.0

    # --- India: Diwali season (Oct 15 - Nov 15) ---------------------------
    if mkt == "IN":
        diwali_start = date(year, 10, 15)
        diwali_end = date(year, 11, 15)
        if _is_offer_loyalty_metric(metric_name):
            multiplier *= _smooth_event(dt, diwali_start, diwali_end, 1.40, ramp_days=5)
        else:
            # General uplift during Diwali even for non-offer metrics
            multiplier *= _smooth_event(dt, diwali_start, diwali_end, 1.10, ramp_days=5)

    # --- Japan: Golden Week (Apr 29 - May 5) ------------------------------
    if mkt == "JP":
        gw_start = date(year, 4, 29)
        gw_end = date(year, 5, 5)
        if _is_gaming(category_id):
            multiplier *= _smooth_event(dt, gw_start, gw_end, 1.30, ramp_days=3)

    # --- US: Black Friday week (last week of November) --------------------
    if mkt == "US":
        # Find Thanksgiving (4th Thursday of November)
        nov1 = date(year, 11, 1)
        # day-of-week for Nov 1: 0=Mon
        first_thu = (3 - nov1.weekday()) % 7 + 1  # day-of-month of first Thu
        thanksgiving_day = first_thu + 21  # 4th Thursday
        bf_start = date(year, 11, thanksgiving_day)  # Thanksgiving day
        bf_end = bf_start + timedelta(days=4)  # through Cyber Monday
        if _is_shopping(category_id):
            multiplier *= _smooth_event(dt, bf_start, bf_end, 1.50, ramp_days=3)
        if _is_offer_loyalty_metric(metric_name):
            multiplier *= _smooth_event(dt, bf_start, bf_end, 1.20, ramp_days=3)

    # --- Brazil: Carnival (approx Feb 10-17) ------------------------------
    if mkt == "BR":
        carnival_start = date(year, 2, 10)
        carnival_end = date(year, 2, 17)
        if _is_entertainment(category_id) or _is_gaming(category_id):
            multiplier *= _smooth_event(dt, carnival_start, carnival_end, 1.20, ramp_days=2)

    # --- South Korea: Chuseok (approx Sep 15-18) -------------------------
    if mkt == "KR":
        chuseok_start = date(year, 9, 15)
        chuseok_end = date(year, 9, 18)
        if _is_gaming(category_id):
            multiplier *= _smooth_event(dt, chuseok_start, chuseok_end, 1.25, ramp_days=2)

    # --- Lunar New Year (approx Jan 25 - Feb 8) --------------------------
    # Affects CN and several Southeast/South Asian markets
    lunar_ny_markets = {"CN", "IN", "ID", "VN", "PH"}
    if mkt in lunar_ny_markets:
        lny_start = date(year, 1, 25)
        lny_end = date(year, 2, 8)
        multiplier *= _smooth_event(dt, lny_start, lny_end, 1.30, ramp_days=4)

    # --- Turkey: Ramadan (approx Mar 10 - Apr 9 for 2024/2025) -----------
    if mkt == "TR":
        ramadan_start = date(year, 3, 10)
        ramadan_end = date(year, 4, 9)
        if _is_entertainment(category_id):
            multiplier *= _smooth_event(dt, ramadan_start, ramadan_end, 0.85, ramp_days=3)
        if _is_finance(category_id):
            multiplier *= _smooth_event(dt, ramadan_start, ramadan_end, 1.10, ramp_days=3)

    # --- DE/GB: Christmas markets (Dec 1-24) ------------------------------
    if mkt in ("DE", "GB"):
        xmas_start = date(year, 12, 1)
        xmas_end = date(year, 12, 24)
        if _is_shopping(category_id):
            multiplier *= _smooth_event(dt, xmas_start, xmas_end, 1.15, ramp_days=3)

    return multiplier


# ---------------------------------------------------------------------------
# 4. Year-end revenue spike (Dec 20-31, all markets)
# ---------------------------------------------------------------------------

def _year_end_multiplier(dt: date, metric_name: str) -> float:
    year = dt.year
    if dt.month != 12 or dt.day < 17:
        return 1.0

    spike_start = date(year, 12, 20)
    spike_end = date(year, 12, 31)

    if _is_revenue_metric(metric_name) or _is_offer_loyalty_metric(metric_name):
        return _smooth_event(dt, spike_start, spike_end, 1.25, ramp_days=3)

    # General uplift for all metrics during the gift-card / app-purchase window
    return _smooth_event(dt, spike_start, spike_end, 1.10, ramp_days=3)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_seasonality(
    date_val: datetime,
    market_id: str,
    category_id: str,
    metric_name: str,
) -> float:
    """Compute a seasonal multiplier for the given date, market, category, and metric.

    Parameters
    ----------
    date_val : datetime
        The date (time component is ignored).
    market_id : str
        ISO-style market code, e.g. ``"US"``, ``"IN"``, ``"JP"``.
    category_id : str
        Play Store category identifier, e.g. ``"GAM_ACTION"``, ``"PROD_OFFICE"``.
    metric_name : str
        Metric name, e.g. ``"daily_revenue"``, ``"offer_redemptions"``.

    Returns
    -------
    float
        Multiplier centered around 1.0. Values > 1.0 indicate seasonal uplift;
        values < 1.0 indicate seasonal dip.
    """
    d = date_val.date() if isinstance(date_val, datetime) else date_val

    m = 1.0
    m *= _day_of_week_multiplier(d, category_id)
    m *= _monthly_multiplier(d, category_id, metric_name)
    m *= _market_event_multiplier(d, market_id, category_id, metric_name)
    m *= _year_end_multiplier(d, metric_name)

    return round(m, 6)


def get_seasonal_baselines(
    markets: list[str],
    categories: list[str],
    metrics: list[str],
) -> dict[str, Any]:
    """Pre-compute seasonal baselines for export to ``seasonal_patterns.json``.

    Generates a summary of seasonal patterns for each metric x market
    combination, suitable for the knowledge layer. For every combination the
    output includes the mean multiplier across the year, the peak month, and
    notable events that affect the combination.

    Parameters
    ----------
    markets : list[str]
        Market codes (e.g. ``["US", "IN", "JP"]``).
    categories : list[str]
        Category identifiers.
    metrics : list[str]
        Metric names.

    Returns
    -------
    dict
        Nested structure::

            {
              "<market_id>": {
                "<category_id>": {
                    "<metric_name>": {
                        "annual_mean": float,
                        "peak_month": int,         # 1-12
                        "peak_multiplier": float,
                        "trough_month": int,
                        "trough_multiplier": float,
                        "notable_events": [str, ...],
                    }
                }
              }
            }
    """
    # Use a reference year for baseline computation (non-leap, starts on Monday)
    ref_year = 2024
    all_days = [
        date(ref_year, 1, 1) + timedelta(days=d) for d in range(366)  # 2024 is leap
    ]

    # Pre-build known events per market for annotation
    _event_annotations = _build_event_annotations()

    result: dict[str, Any] = {}

    for mkt in markets:
        result[mkt] = {}
        for cat in categories:
            result[mkt][cat] = {}
            for metric in metrics:
                monthly_sums: dict[int, float] = {m: 0.0 for m in range(1, 13)}
                monthly_counts: dict[int, int] = {m: 0 for m in range(1, 13)}

                for d in all_days:
                    dt = datetime(d.year, d.month, d.day)
                    m = compute_seasonality(dt, mkt, cat, metric)
                    monthly_sums[d.month] += m
                    monthly_counts[d.month] += 1

                monthly_means = {
                    mo: monthly_sums[mo] / monthly_counts[mo]
                    for mo in range(1, 13)
                }

                annual_mean = sum(monthly_means.values()) / 12.0
                peak_month = max(monthly_means, key=monthly_means.get)  # type: ignore[arg-type]
                trough_month = min(monthly_means, key=monthly_means.get)  # type: ignore[arg-type]

                # Collect notable events for this market/category/metric
                notable = _collect_notable_events(mkt, cat, metric, _event_annotations)

                result[mkt][cat][metric] = {
                    "annual_mean": round(annual_mean, 4),
                    "peak_month": peak_month,
                    "peak_multiplier": round(monthly_means[peak_month], 4),
                    "trough_month": trough_month,
                    "trough_multiplier": round(monthly_means[trough_month], 4),
                    "notable_events": notable,
                }

    return result


# ---------------------------------------------------------------------------
# Internal helpers for get_seasonal_baselines
# ---------------------------------------------------------------------------

def _build_event_annotations() -> list[dict[str, Any]]:
    """Return a list of event descriptors used for annotation."""
    return [
        {
            "name": "Diwali Season",
            "markets": ["IN"],
            "date_range": "Oct 15 - Nov 15",
            "affects": "offer_loyalty",
            "peak": 1.4,
        },
        {
            "name": "Golden Week",
            "markets": ["JP"],
            "date_range": "Apr 29 - May 5",
            "affects": "gaming",
            "peak": 1.3,
        },
        {
            "name": "Black Friday",
            "markets": ["US"],
            "date_range": "Last week of November",
            "affects": "shopping",
            "peak": 1.5,
        },
        {
            "name": "Carnival",
            "markets": ["BR"],
            "date_range": "Feb 10 - Feb 17",
            "affects": "entertainment_gaming",
            "peak": 1.2,
        },
        {
            "name": "Chuseok",
            "markets": ["KR"],
            "date_range": "Sep 15 - Sep 18",
            "affects": "gaming",
            "peak": 1.25,
        },
        {
            "name": "Lunar New Year",
            "markets": ["CN", "IN", "ID", "VN", "PH"],
            "date_range": "Jan 25 - Feb 8",
            "affects": "general",
            "peak": 1.3,
        },
        {
            "name": "Ramadan",
            "markets": ["TR"],
            "date_range": "Mar 10 - Apr 9",
            "affects": "entertainment_finance",
            "peak": "0.85 ent / 1.1 fin",
        },
        {
            "name": "Christmas Markets",
            "markets": ["DE", "GB"],
            "date_range": "Dec 1 - Dec 24",
            "affects": "shopping",
            "peak": 1.15,
        },
        {
            "name": "Year-End Spike",
            "markets": ["ALL"],
            "date_range": "Dec 20 - Dec 31",
            "affects": "revenue",
            "peak": 1.25,
        },
    ]


def _collect_notable_events(
    market_id: str,
    category_id: str,
    metric_name: str,
    annotations: list[dict[str, Any]],
) -> list[str]:
    """Return human-readable event descriptions relevant to this combination."""
    mkt = market_id.upper()
    relevant: list[str] = []

    for evt in annotations:
        markets = evt["markets"]
        if mkt not in markets and "ALL" not in markets:
            continue

        affects = evt["affects"]
        match = False

        if affects == "general":
            match = True
        elif affects == "offer_loyalty" and _is_offer_loyalty_metric(metric_name):
            match = True
        elif affects == "gaming" and _is_gaming(category_id):
            match = True
        elif affects == "shopping" and _is_shopping(category_id):
            match = True
        elif affects == "entertainment_gaming" and (
            _is_entertainment(category_id) or _is_gaming(category_id)
        ):
            match = True
        elif affects == "entertainment_finance" and (
            _is_entertainment(category_id) or _is_finance(category_id)
        ):
            match = True
        elif affects == "revenue" and _is_revenue_metric(metric_name):
            match = True

        if match:
            relevant.append(f"{evt['name']} ({evt['date_range']}): peak {evt['peak']}x")

    return relevant
