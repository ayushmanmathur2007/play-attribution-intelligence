"""
Loyalty & Offers initiatives for the Google Play attribution system.

Defines 18 realistic initiatives spanning Oct 2023 - Mar 2025.
Each initiative has a trapezoidal impact envelope (ramp-up, plateau, decay)
that modulates its effect on target metrics for matching cells.
"""

from datetime import date, datetime, timedelta
from typing import Optional


def _parse_date(d) -> date:
    """Accept date objects or ISO date strings."""
    if isinstance(d, date):
        return d
    return datetime.strptime(d, "%Y-%m-%d").date()


def _trapezoidal_envelope(
    current_date,
    start_date,
    end_date,
    ramp_up_days: int,
    decay_days: int,
) -> float:
    """
    Compute a trapezoidal envelope value in [0.0, 1.0].

    Shape:
        0 ──ramp_up──▶ 1 ──plateau──▶ 1 ──decay──▶ 0

    - During ramp-up (start_date to start_date + ramp_up_days): linearly 0 → 1
    - During plateau (start_date + ramp_up_days to end_date): 1.0
    - During decay (end_date to end_date + decay_days): linearly 1 → 0
    - Outside the full window: 0.0
    """
    current_date = _parse_date(current_date)
    start_date = _parse_date(start_date)
    end_date = _parse_date(end_date)

    # Before the initiative starts
    if current_date < start_date:
        return 0.0

    # After the initiative ends + decay
    decay_end = end_date + timedelta(days=decay_days)
    if current_date > decay_end:
        return 0.0

    # Ramp-up phase
    ramp_end = start_date + timedelta(days=ramp_up_days)
    if current_date < ramp_end:
        days_in = (current_date - start_date).days
        if ramp_up_days == 0:
            return 1.0
        return days_in / ramp_up_days

    # Plateau phase
    if current_date <= end_date:
        return 1.0

    # Decay phase
    if decay_days == 0:
        return 0.0
    days_past_end = (current_date - end_date).days
    return max(0.0, 1.0 - days_past_end / decay_days)


def get_initiatives() -> list[dict]:
    """
    Return the 18 Loyalty & Offers initiatives for the simulation.

    Each initiative dict contains:
        id, name, type, start_date, end_date, target_markets, target_segments,
        target_categories, impact, ramp_up_days, decay_days, experiment_id,
        budget_usd, status
    """
    return [
        # ----------------------------------------------------------------
        # INIT_001 — Diwali Play Points 3x Bonus
        # ----------------------------------------------------------------
        {
            "id": "INIT_001",
            "name": "Diwali Play Points 3x Bonus",
            "type": "PLAY_POINTS_BONUS",
            "start_date": "2024-10-15",
            "end_date": "2024-11-15",
            "target_markets": ["IN"],
            "target_segments": ["ALL"],
            "target_categories": ["ALL"],
            "impact": {
                "play_points_earn_rate": 200.0,
                "play_points_burn_rate": 150.0,
                "offer_redemption_rate": 0.15,
                "loyalty_driven_purchases": 8000,
            },
            "ramp_up_days": 5,
            "decay_days": 7,
            "experiment_id": None,
            "budget_usd": 500_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_002 — Global Holiday Gift Card Bonus
        # ----------------------------------------------------------------
        {
            "id": "INIT_002",
            "name": "Global Holiday Gift Card Bonus",
            "type": "GIFT_CARD_BONUS",
            "start_date": "2024-12-01",
            "end_date": "2024-12-31",
            "target_markets": ["ALL"],
            "target_segments": ["ALL"],
            "target_categories": ["SHO", "ENT"],
            "impact": {
                "offer_driven_revenue": 120_000.0,
                "offer_redemption_count": 5000,
                "offer_cost": 25_000.0,
            },
            "ramp_up_days": 3,
            "decay_days": 10,
            "experiment_id": None,
            "budget_usd": 300_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_003 — Win-back Campaign (Lapsed Users)
        # Negative side effects: revenue_per_user drops, d30_retention drops
        # ----------------------------------------------------------------
        {
            "id": "INIT_003",
            "name": "Win-back Campaign (Lapsed Users)",
            "type": "RE_ENGAGEMENT",
            "start_date": "2024-11-01",
            "end_date": "2024-12-15",
            "target_markets": ["US", "GB", "DE"],
            "target_segments": ["LAP_30_90", "LAP_90"],
            "target_categories": ["ALL"],
            "impact": {
                "wau": 500_000,
                "dau": 200_000,
                "revenue_per_user": -0.8,
                "d30_retention": -0.03,
            },
            "ramp_up_days": 7,
            "decay_days": 14,
            "experiment_id": "EXP_WINBACK_2024Q4",
            "budget_usd": 750_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_004 — Subscription Free Trial Push
        # Delayed impact: metric effect starts 14 days after trial start
        # ----------------------------------------------------------------
        {
            "id": "INIT_004",
            "name": "Subscription Free Trial Push",
            "type": "SUBSCRIPTION_TRIAL",
            "start_date": "2024-09-01",
            "end_date": "2024-10-15",
            "target_markets": ["US", "JP", "KR"],
            "target_segments": ["ACT_HIGH", "ACT_MED"],
            "target_categories": ["ENT", "GAM"],
            "impact": {
                "subscription_conversion_rate": 0.02,
            },
            "ramp_up_days": 14,
            "decay_days": 21,
            "experiment_id": "EXP_SUBTRIAL_2024Q3",
            "budget_usd": 400_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_005 — PAUSED Mid-flight Cashback Campaign
        # Was supposed to run until Sep 15, but paused Aug 20 due to
        # budget overrun. Abrupt stop (decay_days=0).
        # ----------------------------------------------------------------
        {
            "id": "INIT_005",
            "name": "PAUSED Mid-flight Cashback Campaign",
            "type": "CASHBACK",
            "start_date": "2024-08-01",
            "end_date": "2024-08-20",
            "target_markets": ["BR", "MX"],
            "target_segments": ["ALL"],
            "target_categories": ["SHO"],
            "impact": {
                "offer_redemption_rate": 0.08,
                "offer_cost": 40_000.0,
            },
            "ramp_up_days": 3,
            "decay_days": 0,
            "experiment_id": None,
            "budget_usd": 200_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_006 — Japan Golden Week Games Promo
        # ----------------------------------------------------------------
        {
            "id": "INIT_006",
            "name": "Japan Golden Week Games Promo",
            "type": "DEVELOPER_PROMO",
            "start_date": "2024-04-27",
            "end_date": "2024-05-07",
            "target_markets": ["JP"],
            "target_segments": ["ALL"],
            "target_categories": ["GAM_CAS", "GAM_MID", "GAM_HRD", "GAM_RPG", "GAM_STR"],
            "impact": {
                "sessions_per_user": 2.5,
                "avg_session_duration": 5.0,
                "dau": 1_500_000,
            },
            "ramp_up_days": 2,
            "decay_days": 5,
            "experiment_id": None,
            "budget_usd": 350_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_007 — India Republic Day Offer Blitz
        # ----------------------------------------------------------------
        {
            "id": "INIT_007",
            "name": "India Republic Day Offer Blitz",
            "type": "CASHBACK",
            "start_date": "2024-01-20",
            "end_date": "2024-02-05",
            "target_markets": ["IN"],
            "target_segments": ["ALL"],
            "target_categories": ["ALL"],
            "impact": {
                "offer_impression_count": 80_000,
                "offer_ctr": 0.04,
                "offer_funnel_conversion": 0.01,
            },
            "ramp_up_days": 3,
            "decay_days": 7,
            "experiment_id": None,
            "budget_usd": 250_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_008 — Play Points Tier Upgrade Event
        # ----------------------------------------------------------------
        {
            "id": "INIT_008",
            "name": "Play Points Tier Upgrade Event",
            "type": "LOYALTY_TIER_UP",
            "start_date": "2024-03-01",
            "end_date": "2024-03-31",
            "target_markets": ["ALL"],
            "target_segments": ["PP_ACTIVE", "PP_DORMANT"],
            "target_categories": ["ALL"],
            "impact": {
                "play_points_earn_rate": 25.0,
                "play_points_burn_rate": 15.0,
                "loyalty_driven_purchases": 3000,
            },
            "ramp_up_days": 5,
            "decay_days": 10,
            "experiment_id": None,
            "budget_usd": 180_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_009 — US Back-to-School Bundle
        # ----------------------------------------------------------------
        {
            "id": "INIT_009",
            "name": "US Back-to-School Bundle",
            "type": "BUNDLE_OFFER",
            "start_date": "2024-08-15",
            "end_date": "2024-09-15",
            "target_markets": ["US"],
            "target_segments": ["NEW_0_7", "NEW_8_30"],
            "target_categories": ["EDU", "PRD"],
            "impact": {
                "store_visit_to_install_rate": 0.05,
                "install_to_first_purchase_rate": 0.008,
            },
            "ramp_up_days": 3,
            "decay_days": 7,
            "experiment_id": None,
            "budget_usd": 150_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_010 — Korea Chuseok Gaming Festival
        # ----------------------------------------------------------------
        {
            "id": "INIT_010",
            "name": "Korea Chuseok Gaming Festival",
            "type": "DEVELOPER_PROMO",
            "start_date": "2024-09-14",
            "end_date": "2024-09-20",
            "target_markets": ["KR"],
            "target_segments": ["ALL"],
            "target_categories": ["GAM_CAS", "GAM_MID", "GAM_HRD", "GAM_RPG", "GAM_STR"],
            "impact": {
                "dau": 800_000,
                "sessions_per_user": 3.0,
                "offer_redemption_count": 2000,
            },
            "ramp_up_days": 2,
            "decay_days": 4,
            "experiment_id": None,
            "budget_usd": 200_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_011 — Black Friday Global Mega Sale
        # ----------------------------------------------------------------
        {
            "id": "INIT_011",
            "name": "Black Friday Global Mega Sale",
            "type": "CASHBACK",
            "start_date": "2024-11-25",
            "end_date": "2024-12-02",
            "target_markets": ["ALL"],
            "target_segments": ["ALL"],
            "target_categories": ["SHO", "ENT"],
            "impact": {
                "offer_driven_revenue": 250_000.0,
                "offer_impression_count": 100_000,
                "offer_redemption_rate": 0.12,
            },
            "ramp_up_days": 1,
            "decay_days": 5,
            "experiment_id": None,
            "budget_usd": 600_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_012 — Developer Promo: Indie Games Spotlight
        # ----------------------------------------------------------------
        {
            "id": "INIT_012",
            "name": "Developer Promo: Indie Games Spotlight",
            "type": "DEVELOPER_PROMO",
            "start_date": "2024-07-01",
            "end_date": "2024-07-31",
            "target_markets": ["US", "GB", "DE", "JP"],
            "target_segments": ["ALL"],
            "target_categories": ["GAM_CAS", "GAM_MID"],
            "impact": {
                "store_visit_to_install_rate": 0.03,
                "dau": 300_000,
            },
            "ramp_up_days": 5,
            "decay_days": 10,
            "experiment_id": "EXP_INDIE_2024Q3",
            "budget_usd": 220_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_013 — Ramadan Special Offers
        # ----------------------------------------------------------------
        {
            "id": "INIT_013",
            "name": "Ramadan Special Offers",
            "type": "CASHBACK",
            "start_date": "2024-03-10",
            "end_date": "2024-04-09",
            "target_markets": ["TR", "ID", "EG"],
            "target_segments": ["ALL"],
            "target_categories": ["ALL"],
            "impact": {
                "offer_redemption_rate": 0.06,
                "loyalty_driven_purchases": 4000,
            },
            "ramp_up_days": 5,
            "decay_days": 7,
            "experiment_id": None,
            "budget_usd": 280_000.0,
            "status": "COMPLETED",
        },
        # ----------------------------------------------------------------
        # INIT_014 — New Year Loyalty Boost
        # ----------------------------------------------------------------
        {
            "id": "INIT_014",
            "name": "New Year Loyalty Boost",
            "type": "PLAY_POINTS_BONUS",
            "start_date": "2024-12-28",
            "end_date": "2025-01-15",
            "target_markets": ["ALL"],
            "target_segments": ["ALL"],
            "target_categories": ["ALL"],
            "impact": {
                "play_points_earn_rate": 30.0,
                "offer_impression_count": 50_000,
            },
            "ramp_up_days": 3,
            "decay_days": 10,
            "experiment_id": None,
            "budget_usd": 200_000.0,
            "status": "ACTIVE",
        },
        # ----------------------------------------------------------------
        # INIT_015 — Valentine's Day Gift Card Push
        # ----------------------------------------------------------------
        {
            "id": "INIT_015",
            "name": "Valentine's Day Gift Card Push",
            "type": "GIFT_CARD_BONUS",
            "start_date": "2025-02-07",
            "end_date": "2025-02-16",
            "target_markets": ["US", "GB", "JP", "KR"],
            "target_segments": ["ALL"],
            "target_categories": ["ENT", "SOC"],
            "impact": {
                "offer_driven_revenue": 60_000.0,
                "offer_cost": 12_000.0,
            },
            "ramp_up_days": 2,
            "decay_days": 5,
            "experiment_id": None,
            "budget_usd": 120_000.0,
            "status": "ACTIVE",
        },
        # ----------------------------------------------------------------
        # INIT_016 — Southeast Asia Growth Push
        # ----------------------------------------------------------------
        {
            "id": "INIT_016",
            "name": "Southeast Asia Growth Push",
            "type": "RE_ENGAGEMENT",
            "start_date": "2025-01-15",
            "end_date": "2025-03-15",
            "target_markets": ["ID", "PH", "VN"],
            "target_segments": ["ALL"],
            "target_categories": ["ALL"],
            "impact": {
                "wau": 2_000_000,
                "store_visit_to_install_rate": 0.04,
                "offer_impression_count": 60_000,
            },
            "ramp_up_days": 10,
            "decay_days": 14,
            "experiment_id": "EXP_SEA_GROWTH_2025Q1",
            "budget_usd": 900_000.0,
            "status": "ACTIVE",
        },
        # ----------------------------------------------------------------
        # INIT_017 — Premium Tier Retention Program
        # ----------------------------------------------------------------
        {
            "id": "INIT_017",
            "name": "Premium Tier Retention Program",
            "type": "LOYALTY_TIER_UP",
            "start_date": "2025-02-01",
            "end_date": "2025-03-31",
            "target_markets": ["ALL"],
            "target_segments": ["ACT_HIGH"],
            "target_categories": ["ALL"],
            "impact": {
                "d7_retention": 0.05,
                "d30_retention": 0.03,
                "churn_rate": -0.02,
            },
            "ramp_up_days": 7,
            "decay_days": 14,
            "experiment_id": None,
            "budget_usd": 350_000.0,
            "status": "ACTIVE",
        },
        # ----------------------------------------------------------------
        # INIT_018 — Brazil Carnival Gaming Blast
        # ----------------------------------------------------------------
        {
            "id": "INIT_018",
            "name": "Brazil Carnival Gaming Blast",
            "type": "DEVELOPER_PROMO",
            "start_date": "2025-02-28",
            "end_date": "2025-03-05",
            "target_markets": ["BR"],
            "target_segments": ["ALL"],
            "target_categories": ["GAM_CAS", "GAM_MID", "GAM_HRD", "GAM_RPG", "GAM_STR"],
            "impact": {
                "sessions_per_user": 4.0,
                "dau": 600_000,
            },
            "ramp_up_days": 1,
            "decay_days": 3,
            "experiment_id": None,
            "budget_usd": 160_000.0,
            "status": "PLANNED",
        },
    ]


def compute_initiative_impact(
    current_date,
    market_id: str,
    category_id: str,
    segment_id: str,
    metric_name: str,
    initiatives: Optional[list[dict]] = None,
) -> float:
    """
    Sum of all active initiative impacts for a given cell.

    An initiative applies to a cell if:
    - market_id is in target_markets (or target_markets == ["ALL"])
    - category_id is in target_categories (or target_categories == ["ALL"])
    - segment_id is in target_segments (or target_segments == ["ALL"])
    - metric_name is in the initiative's impact dict
    - The date falls within [start_date, end_date + decay_days]

    Returns the raw impact value scaled by the trapezoidal envelope.
    The caller handles whether this is additive or multiplicative.
    """
    if initiatives is None:
        initiatives = get_initiatives()

    current_date = _parse_date(current_date)
    total_impact = 0.0

    for init in initiatives:
        # Check metric match first (cheapest check)
        if metric_name not in init["impact"]:
            continue

        # Check market match
        if init["target_markets"] != ["ALL"] and market_id not in init["target_markets"]:
            continue

        # Check category match
        if init["target_categories"] != ["ALL"] and category_id not in init["target_categories"]:
            continue

        # Check segment match
        if init["target_segments"] != ["ALL"] and segment_id not in init["target_segments"]:
            continue

        # Compute envelope
        envelope = _trapezoidal_envelope(
            current_date,
            init["start_date"],
            init["end_date"],
            init["ramp_up_days"],
            init["decay_days"],
        )

        if envelope > 0.0:
            total_impact += init["impact"][metric_name] * envelope

    return total_impact
