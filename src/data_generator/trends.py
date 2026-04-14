"""Structural (long-term) trend models for synthetic data generation."""

from datetime import datetime
import numpy as np
import pandas as pd

DATA_START = datetime(2023, 10, 1)

TIER_MAP = {
    "US": 1, "GB": 1, "DE": 1, "JP": 1, "KR": 1,
    "BR": 2, "IN": 2, "MX": 2, "ID": 2, "TR": 2,
    "RU": 3, "NG": 3, "PH": 3, "EG": 3, "VN": 3,
}


def _months_elapsed(date: datetime) -> float:
    delta = date - DATA_START
    return delta.days / 30.44


def _get_market_tier(market_id: str) -> int:
    return TIER_MAP.get(market_id, 2)


# Monthly growth rates per metric category, indexed by tier (1, 2, 3)
TREND_RATES: dict[str, dict[int, float]] = {
    # Engagement metrics — platform growth
    "wau":                      {1: 0.005, 2: 0.015, 3: 0.025},
    "dau":                      {1: 0.005, 2: 0.015, 3: 0.025},
    "sessions_per_user":        {1: 0.003, 2: 0.008, 3: 0.012},
    "avg_session_duration":     {1: 0.002, 2: 0.005, 3: 0.008},

    # Offer maturation — habituation
    "offer_ctr":                {1: -0.003, 2: -0.003, 3: -0.003},
    "offer_redemption_rate":    {1: -0.003, 2: -0.003, 3: -0.003},
    "offer_impression_count":   {1: 0.010, 2: 0.012, 3: 0.015},
    "offer_redemption_count":   {1: 0.005, 2: 0.008, 3: 0.010},
    "offer_driven_revenue":     {1: 0.005, 2: 0.008, 3: 0.010},
    "offer_cost":               {1: 0.004, 2: 0.006, 3: 0.008},
    "offer_roi":                {1: 0.001, 2: 0.002, 3: 0.002},
    "avg_time_to_redemption":   {1: -0.002, 2: -0.003, 3: -0.004},
    "offer_funnel_conversion":  {1: -0.002, 2: -0.002, 3: -0.002},

    # Loyalty program growth
    "play_points_earn_rate":    {1: 0.010, 2: 0.010, 3: 0.010},
    "play_points_burn_rate":    {1: 0.010, 2: 0.010, 3: 0.010},
    "play_points_balance_avg":  {1: 0.008, 2: 0.008, 3: 0.008},
    "loyalty_driven_purchases": {1: 0.010, 2: 0.012, 3: 0.015},

    # Revenue trends
    "revenue_per_user":         {1: 0.002, 2: 0.008, 3: 0.008},
    "ltv_30d":                  {1: 0.002, 2: 0.008, 3: 0.008},

    # Retention pressure
    "d7_retention":             {1: -0.001, 2: -0.001, 3: -0.001},
    "d30_retention":            {1: -0.001, 2: -0.001, 3: -0.001},
    "churn_rate":               {1: 0.001, 2: 0.001, 3: 0.001},

    # Funnel optimization
    "store_visit_to_install_rate":    {1: 0.0015, 2: 0.002, 3: 0.0025},
    "install_to_first_purchase_rate": {1: 0.0005, 2: 0.0005, 3: 0.0005},
    "subscription_conversion_rate":   {1: 0.001, 2: 0.001, 3: 0.001},
}


def compute_structural_trend(date: datetime, metric_name: str, market_id: str) -> float:
    """Return a multiplier representing long-term structural trend.

    Starts at 1.0 on DATA_START and grows/declines via compound monthly rate.
    """
    months = _months_elapsed(date)
    tier = _get_market_tier(market_id)

    rates = TREND_RATES.get(metric_name)
    if rates is None:
        return 1.0

    monthly_rate = rates.get(tier, rates.get(2, 0.0))
    return (1.0 + monthly_rate) ** months


def detect_change_points(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Detect structural change points in time series data.

    Aggregates daily data to weekly by market × metric, computes WoW % changes,
    and flags points where |z-score| > 2.0.
    """
    if daily_df.empty:
        return pd.DataFrame(columns=["date", "metric_name", "market_id", "magnitude", "direction"])

    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"] = df["date"].dt.year

    # Aggregate to weekly
    weekly = (
        df.groupby(["year", "week", "market_id", "metric_name"])["value"]
        .mean()
        .reset_index()
    )
    weekly = weekly.sort_values(["market_id", "metric_name", "year", "week"])

    change_points = []

    for (market, metric), group in weekly.groupby(["market_id", "metric_name"]):
        if len(group) < 5:
            continue

        values = group["value"].values
        # WoW percentage changes
        pct_changes = np.diff(values) / (np.abs(values[:-1]) + 1e-10)

        if len(pct_changes) < 3:
            continue

        mean_change = np.mean(pct_changes)
        std_change = np.std(pct_changes)

        if std_change < 1e-10:
            continue

        z_scores = (pct_changes - mean_change) / std_change

        for i, z in enumerate(z_scores):
            if abs(z) > 2.0:
                row = group.iloc[i + 1]
                # Approximate the date from year/week
                approx_date = datetime.strptime(f"{int(row['year'])}-W{int(row['week']):02d}-1", "%Y-W%W-%w")
                change_points.append({
                    "date": approx_date.strftime("%Y-%m-%d"),
                    "metric_name": metric,
                    "market_id": market,
                    "magnitude": float(pct_changes[i]),
                    "direction": "increase" if pct_changes[i] > 0 else "decrease",
                })

    result = pd.DataFrame(change_points)
    if result.empty:
        result = pd.DataFrame(columns=["date", "metric_name", "market_id", "magnitude", "direction"])
    return result
