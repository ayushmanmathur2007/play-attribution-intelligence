"""
External confounders (non-initiative events) for the Google Play data generator.

Defines events like competitor launches, policy changes, data pipeline issues,
viral events, macroeconomic shifts, and algorithm changes that affect metrics
independently of any initiative the team is running.
"""

from datetime import datetime, timedelta


def get_confounders() -> list[dict]:
    """Return the list of external confounder events."""
    return [
        # 1. Data Pipeline Delay
        {
            "name": "Data Pipeline Delay",
            "type": "DATA_PIPELINE_ISSUE",
            "date": "2024-11-08",
            "duration_days": 2,
            "affected_markets": ["ALL"],
            "affected_categories": ["ALL"],
            "impact": {
                "dau": -0.15,
                "wau": -0.10,
                "offer_redemption_count": -0.12,
            },
            "is_data_artifact": True,
            "description": (
                "Data pipeline ingestion delay caused 2-day reporting gap. "
                "Metrics recovered automatically when pipeline backfill completed."
            ),
        },
        # 2. Competitor Major Launch
        {
            "name": "Competitor Major Launch",
            "type": "COMPETITOR_LAUNCH",
            "date": "2024-10-01",
            "duration_days": 21,
            "affected_markets": ["US", "GB"],
            "affected_categories": ["GAM_CAS", "GAM_MID", "APP_ENT"],
            "impact": {
                "dau": -300000,
                "d7_retention": -0.02,
                "store_visit_to_install_rate": -0.03,
            },
            "is_data_artifact": False,
            "description": (
                "Major competitor launched Play Pass alternative with "
                "3-month free trial in US/GB."
            ),
        },
        # 3. Platform Policy Change
        {
            "name": "Platform Policy Change",
            "type": "POLICY_CHANGE",
            "date": "2024-01-15",
            "duration_days": 999,
            "affected_markets": ["ALL"],
            "affected_categories": ["ALL"],
            "impact": {
                "install_to_first_purchase_rate": -0.005,
                "offer_ctr": -0.01,
            },
            "is_data_artifact": False,
            "description": (
                "New billing transparency regulation requires explicit price "
                "confirmation step, adding friction to purchase flow."
            ),
        },
        # 4. Organic Viral Event
        {
            "name": "Organic Viral Event",
            "type": "ORGANIC_VIRAL",
            "date": "2024-07-10",
            "duration_days": 7,
            "affected_markets": ["US", "GB", "BR", "IN"],
            "affected_categories": ["GAM_HYP"],
            "impact": {
                "dau": 5000000,
                "store_visit_to_install_rate": 0.08,
                "sessions_per_user": 2.0,
            },
            "is_data_artifact": False,
            "description": (
                "Viral TikTok challenge featuring a hypercasual game "
                "drove massive download spike."
            ),
        },
        # 5. Macroeconomic Event - Currency Devaluation
        {
            "name": "Macroeconomic Event - Currency Devaluation",
            "type": "MACROECONOMIC",
            "date": "2024-03-01",
            "duration_days": 60,
            "affected_markets": ["TR"],
            "affected_categories": ["ALL"],
            "impact": {
                "revenue_per_user": -0.25,
                "offer_driven_revenue": -0.20,
                "ltv_30d": -0.30,
            },
            "is_data_artifact": False,
            "description": (
                "Turkish Lira devaluation of ~15% reduced "
                "USD-denominated revenue metrics."
            ),
        },
        # 6. App Store Algorithm Change
        {
            "name": "App Store Algorithm Change",
            "type": "APP_STORE_ALGORITHM_CHANGE",
            "date": "2024-05-15",
            "duration_days": 30,
            "affected_markets": ["ALL"],
            "affected_categories": ["GAM_CAS", "GAM_HYP", "APP_EDU"],
            "impact": {
                "store_visit_to_install_rate": 0.04,
                "offer_impression_count": 20000,
            },
            "is_data_artifact": False,
            "description": (
                "Play Store recommendation algorithm update boosted "
                "visibility of casual/educational apps."
            ),
        },
        # 7. Regional Data Center Outage
        {
            "name": "Regional Data Center Outage",
            "type": "DATA_PIPELINE_ISSUE",
            "date": "2024-12-20",
            "duration_days": 1,
            "affected_markets": ["JP", "KR", "ID", "PH", "VN"],
            "affected_categories": ["ALL"],
            "impact": {
                "dau": -0.08,
                "offer_redemption_count": -0.10,
            },
            "is_data_artifact": True,
            "description": (
                "APAC data center maintenance window caused partial "
                "data loss for December 20th."
            ),
        },
        # 8. Nigeria Mobile Payment Integration
        {
            "name": "Nigeria Mobile Payment Integration",
            "type": "POLICY_CHANGE",
            "date": "2025-02-01",
            "duration_days": 45,
            "affected_markets": ["NG"],
            "affected_categories": ["ALL"],
            "impact": {
                "install_to_first_purchase_rate": 0.012,
                "revenue_per_user": 0.15,
                "offer_redemption_rate": 0.05,
            },
            "is_data_artifact": False,
            "description": (
                "Integration with local mobile money platform dramatically "
                "improved payment accessibility in Nigeria."
            ),
        },
    ]


def _parse_date(date_str: str) -> datetime:
    """Parse an ISO date string to a datetime object."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def _is_market_affected(market_id: str, affected_markets: list) -> bool:
    """Check whether a market is affected by a confounder."""
    return "ALL" in affected_markets or market_id in affected_markets


def _is_category_affected(category_id: str, affected_categories: list) -> bool:
    """Check whether a category is affected by a confounder."""
    return "ALL" in affected_categories or category_id in affected_categories


def compute_confounder_impact(
    date: str,
    market_id: str,
    category_id: str,
    metric_name: str,
    confounders: list[dict],
) -> float:
    """
    Sum of all active confounder impacts for a given cell.

    Checks whether each confounder is active on the given date, affects the
    specified market and category, and has an impact defined for the metric.

    For permanent confounders (duration_days == 999), the effect is applied from
    the confounder's start date onward with a 14-day linear ramp-up.

    Returns the raw impact value. The caller decides how to apply it (additive
    vs multiplicative) based on metric semantics.

    Args:
        date: ISO date string (e.g. "2024-06-15").
        market_id: Market identifier (e.g. "US", "TR").
        category_id: App category identifier (e.g. "GAM_CAS", "APP_EDU").
        metric_name: Name of the metric being computed (e.g. "dau").
        confounders: List of confounder dicts as returned by get_confounders().

    Returns:
        The total impact value (float). Can be positive or negative.
    """
    current_date = _parse_date(date)
    total_impact = 0.0

    for confounder in confounders:
        # Check if this confounder affects the given metric
        if metric_name not in confounder["impact"]:
            continue

        # Check market and category match
        if not _is_market_affected(market_id, confounder["affected_markets"]):
            continue
        if not _is_category_affected(category_id, confounder["affected_categories"]):
            continue

        start_date = _parse_date(confounder["date"])
        duration = confounder["duration_days"]
        raw_impact = confounder["impact"][metric_name]

        if duration == 999:
            # Permanent confounder: apply from start_date onward with a
            # 14-day linear ramp from 0 to full impact.
            if current_date < start_date:
                continue
            days_since_start = (current_date - start_date).days
            ramp_days = 14
            if days_since_start < ramp_days:
                ramp_factor = (days_since_start + 1) / ramp_days
            else:
                ramp_factor = 1.0
            total_impact += raw_impact * ramp_factor
        else:
            # Finite-duration confounder: active within [start, start + duration)
            end_date = start_date + timedelta(days=duration)
            if start_date <= current_date < end_date:
                total_impact += raw_impact

    return total_impact


# -- Rate-like metric names used to distinguish additive rate impacts from
#    multiplicative percentage impacts.
_RATE_METRICS = frozenset({
    "d7_retention",
    "d30_retention",
    "store_visit_to_install_rate",
    "install_to_first_purchase_rate",
    "offer_ctr",
    "offer_redemption_rate",
    "sessions_per_user",
})


def is_confounder_impact_multiplicative(confounder: dict, metric_name: str) -> bool:
    """
    Return True if the confounder's impact on the given metric is multiplicative.

    Heuristic:
      - MACROECONOMIC and DATA_PIPELINE_ISSUE confounders apply multiplicative
        impacts (percentage reductions/increases on the base value).
      - For other confounder types, if the metric is a rate-like metric and
        the impact magnitude is between -1 and 1, the impact is additive
        (a direct shift to the rate value, not a percentage of it).
      - Otherwise (large absolute values like DAU shifts of +/-300,000)
        the impact is additive by nature (raw count adjustment), so we
        return False.

    Args:
        confounder: A single confounder dict.
        metric_name: The metric being affected.

    Returns:
        True for multiplicative impact, False for additive.
    """
    confounder_type = confounder["type"]

    # Data pipeline issues and macroeconomic events are typically expressed
    # as multiplicative factors (e.g. -0.15 means "15% lower").
    if confounder_type in ("MACROECONOMIC", "DATA_PIPELINE_ISSUE"):
        return True

    # For all other types, check whether the impact looks like a rate-level
    # additive shift vs a multiplicative percentage.
    impact_value = confounder["impact"].get(metric_name)
    if impact_value is None:
        return False

    # Rate metrics with small impact values are additive shifts to the rate
    # itself (e.g. store_visit_to_install_rate += 0.04), not multiplicative.
    if metric_name in _RATE_METRICS and -1 < impact_value < 1:
        return False

    # Large absolute impacts (e.g. dau: +5_000_000) are additive counts.
    return False
