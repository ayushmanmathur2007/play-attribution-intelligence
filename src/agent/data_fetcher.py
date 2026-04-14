"""Stage 2: Fetch relevant data from DuckDB based on parsed query parameters."""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Defines which metrics are closely related and should be fetched together
# for cross-metric analysis.
METRIC_ADJACENCY: dict[str, list[str]] = {
    "offer_redemption_rate": [
        "offer_ctr",
        "offer_driven_revenue",
        "offer_impression_count",
        "offer_redemption_count",
    ],
    "offer_ctr": [
        "offer_impression_count",
        "offer_redemption_rate",
    ],
    "offer_driven_revenue": [
        "offer_redemption_rate",
        "offer_cost",
        "offer_roi",
    ],
    "offer_impression_count": [
        "offer_ctr",
        "offer_redemption_rate",
        "sessions_per_user",
    ],
    "offer_redemption_count": [
        "offer_redemption_rate",
        "offer_driven_revenue",
        "offer_ctr",
    ],
    "offer_cost": [
        "offer_driven_revenue",
        "offer_roi",
        "offer_redemption_count",
    ],
    "offer_roi": [
        "offer_driven_revenue",
        "offer_cost",
        "offer_redemption_rate",
    ],
    "avg_time_to_redemption": [
        "offer_redemption_rate",
        "offer_ctr",
        "offer_impression_count",
    ],
    "dau": [
        "wau",
        "sessions_per_user",
        "d7_retention",
    ],
    "wau": [
        "dau",
        "d7_retention",
        "d30_retention",
    ],
    "d7_retention": [
        "d30_retention",
        "wau",
        "churn_rate",
    ],
    "d30_retention": [
        "d7_retention",
        "wau",
        "churn_rate",
        "ltv_30d",
    ],
    "revenue_per_user": [
        "ltv_30d",
        "offer_driven_revenue",
        "loyalty_driven_purchases",
    ],
    "ltv_30d": [
        "revenue_per_user",
        "d30_retention",
        "d7_retention",
    ],
    "sessions_per_user": [
        "dau",
        "avg_session_duration",
        "offer_impression_count",
    ],
    "avg_session_duration": [
        "sessions_per_user",
        "dau",
    ],
    "play_points_earn_rate": [
        "play_points_burn_rate",
        "loyalty_driven_purchases",
        "play_points_balance_avg",
    ],
    "play_points_burn_rate": [
        "play_points_earn_rate",
        "loyalty_driven_purchases",
        "play_points_balance_avg",
    ],
    "play_points_balance_avg": [
        "play_points_earn_rate",
        "play_points_burn_rate",
    ],
    "loyalty_driven_purchases": [
        "play_points_burn_rate",
        "offer_driven_revenue",
        "revenue_per_user",
    ],
    "store_visit_to_install_rate": [
        "install_to_first_purchase_rate",
        "dau",
    ],
    "install_to_first_purchase_rate": [
        "store_visit_to_install_rate",
        "revenue_per_user",
        "ltv_30d",
    ],
    "offer_funnel_conversion": [
        "offer_ctr",
        "offer_redemption_rate",
        "offer_impression_count",
    ],
    "subscription_conversion_rate": [
        "revenue_per_user",
        "churn_rate",
        "ltv_30d",
    ],
    "churn_rate": [
        "d7_retention",
        "d30_retention",
        "dau",
        "wau",
    ],
}

# Top markets to use for cross-market comparison when no specific list is given
DEFAULT_CROSS_MARKETS = ["US", "GB", "DE", "JP", "KR", "BR", "IN"]


class DataFetcher:
    """Fetch all data slices needed for attribution reasoning."""

    def __init__(self, data_client):
        self.db = data_client

    def fetch(self, parsed_query: dict) -> dict:
        """Fetch relevant data from DuckDB based on parsed query.

        Returns dict with keys:
            primary_data, adjacent_metrics, cross_market,
            initiatives, confounders, change_points.
        """
        metric = parsed_query.get("metric", "")
        market = parsed_query.get("market", "ALL")
        category = parsed_query.get("category", "ALL")
        segment = parsed_query.get("segment", "ALL")
        period = parsed_query.get("period", {})
        start_date = period.get("start_date", "")
        end_date = period.get("end_date", "")

        result: dict[str, Any] = {}

        result["primary_data"] = self._fetch_primary(
            metric, market, category, segment, start_date, end_date
        )
        result["adjacent_metrics"] = self._fetch_adjacent(
            metric, market, category, segment, start_date, end_date
        )
        result["cross_market"] = self._fetch_cross_market(
            metric, market, category, segment, start_date, end_date
        )
        result["initiatives"] = self._fetch_initiatives(
            market, category, segment, start_date, end_date
        )
        result["confounders"] = self._fetch_confounders(
            market, category, segment, start_date, end_date
        )
        result["change_points"] = self._fetch_change_points(
            metric, market, category, segment, start_date, end_date
        )

        return result

    # ------------------------------------------------------------------
    # Primary metric data
    # ------------------------------------------------------------------

    def _fetch_primary(
        self,
        metric: str,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch the target metric for the specified dimensions and period.

        Includes a year-over-year comparison by also pulling the same period
        from the previous year.
        """
        where_clauses = [f"metric_name = '{metric}'"]
        if market != "ALL":
            where_clauses.append(f"market_id = '{market}'")
        if category != "ALL":
            where_clauses.append(f"category_id = '{category}'")
        if segment != "ALL":
            where_clauses.append(f"segment_id = '{segment}'")

        date_filter = (
            f"(date BETWEEN '{start_date}' AND '{end_date}')"
        )
        # YoY: same calendar range shifted back 1 year
        yoy_start = _shift_year(start_date, -1)
        yoy_end = _shift_year(end_date, -1)
        yoy_filter = f"(date BETWEEN '{yoy_start}' AND '{yoy_end}')"

        where_base = " AND ".join(where_clauses)
        sql = (
            f"SELECT *, "
            f"CASE WHEN date BETWEEN '{start_date}' AND '{end_date}' "
            f"     THEN 'current' ELSE 'yoy' END AS period_label "
            f"FROM daily_metrics "
            f"WHERE {where_base} AND ({date_filter} OR {yoy_filter}) "
            f"ORDER BY date"
        )
        return self._safe_query(sql)

    # ------------------------------------------------------------------
    # Adjacent metrics
    # ------------------------------------------------------------------

    def _fetch_adjacent(
        self,
        metric: str,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch metrics that are closely related to the primary metric."""
        adjacent = METRIC_ADJACENCY.get(metric, [])
        if not adjacent:
            return pd.DataFrame()

        metric_list = ", ".join(f"'{m}'" for m in adjacent)
        where_clauses = [f"metric_name IN ({metric_list})"]
        if market != "ALL":
            where_clauses.append(f"market_id = '{market}'")
        if category != "ALL":
            where_clauses.append(f"category_id = '{category}'")
        if segment != "ALL":
            where_clauses.append(f"segment_id = '{segment}'")
        where_clauses.append(f"date BETWEEN '{start_date}' AND '{end_date}'")

        sql = (
            f"SELECT * FROM daily_metrics "
            f"WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY metric_name, date"
        )
        return self._safe_query(sql)

    # ------------------------------------------------------------------
    # Cross-market comparison
    # ------------------------------------------------------------------

    def _fetch_cross_market(
        self,
        metric: str,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch the same metric across other top markets for comparison."""
        # Skip if already querying all markets
        if market == "ALL":
            return pd.DataFrame()

        comparison_markets = [m for m in DEFAULT_CROSS_MARKETS if m != market][:5]
        if not comparison_markets:
            return pd.DataFrame()

        market_list = ", ".join(f"'{m}'" for m in comparison_markets)
        where_clauses = [
            f"metric_name = '{metric}'",
            f"market_id IN ({market_list})",
        ]
        if category != "ALL":
            where_clauses.append(f"category_id = '{category}'")
        if segment != "ALL":
            where_clauses.append(f"segment_id = '{segment}'")
        where_clauses.append(f"date BETWEEN '{start_date}' AND '{end_date}'")

        sql = (
            f"SELECT * FROM daily_metrics "
            f"WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY market_id, date"
        )
        return self._safe_query(sql)

    # ------------------------------------------------------------------
    # Initiatives
    # ------------------------------------------------------------------

    def _fetch_initiatives(
        self,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch initiatives active during the query period, then filter
        by market/category/segment in Python (since initiative_calendar
        may store these as comma-separated or JSON lists).
        """
        sql = (
            f"SELECT * FROM initiative_calendar "
            f"WHERE start_date <= '{end_date}' AND end_date >= '{start_date}' "
            f"ORDER BY start_date"
        )
        df = self._safe_query(sql)
        if df.empty:
            return df

        # Filter by market if specified
        market_col = "target_markets" if "target_markets" in df.columns else "market_id"
        if market != "ALL" and market_col in df.columns:
            df = df[
                df[market_col].apply(
                    lambda v: _dimension_matches(v, market)
                )
            ]

        # Filter by category if specified
        cat_col = "target_categories" if "target_categories" in df.columns else "category_id"
        if category != "ALL" and cat_col in df.columns:
            df = df[
                df[cat_col].apply(
                    lambda v: _dimension_matches(v, category)
                )
            ]

        # Filter by segment if specified
        seg_col = "target_segments" if "target_segments" in df.columns else "segment_id"
        if segment != "ALL" and seg_col in df.columns:
            df = df[
                df[seg_col].apply(
                    lambda v: _dimension_matches(v, segment)
                )
            ]

        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Confounders
    # ------------------------------------------------------------------

    def _fetch_confounders(
        self,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch known confounders (external events, pipeline issues, etc.)
        active during the query period.
        """
        sql = (
            f"SELECT * FROM confounder_log "
            f"WHERE date <= '{end_date}' "
            f"AND CAST(date AS DATE) + CAST(duration_days AS INTEGER) >= CAST('{start_date}' AS DATE) "
            f"ORDER BY date"
        )
        df = self._safe_query(sql)
        if df.empty:
            return df

        # Filter by market if specified
        if market != "ALL" and "affected_markets" in df.columns:
            df = df[
                df["affected_markets"].apply(
                    lambda v: _dimension_matches(v, market)
                )
            ]

        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Change points
    # ------------------------------------------------------------------

    def _fetch_change_points(
        self,
        metric: str,
        market: str,
        category: str,
        segment: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Fetch detected change points near the query period."""
        where_clauses = [f"metric_name = '{metric}'"]
        if market != "ALL":
            where_clauses.append(f"market_id = '{market}'")
        if category != "ALL":
            where_clauses.append(f"category_id = '{category}'")
        if segment != "ALL":
            where_clauses.append(f"segment_id = '{segment}'")

        # Widen the window by 7 days on each side to catch nearby changes
        where_clauses.append(
            f"date BETWEEN DATE '{start_date}' - INTERVAL 7 DAY "
            f"AND DATE '{end_date}' + INTERVAL 7 DAY"
        )

        sql = (
            f"SELECT * FROM change_points "
            f"WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY date"
        )
        return self._safe_query(sql)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _safe_query(self, sql: str) -> pd.DataFrame:
        """Execute a query and return results, returning an empty DataFrame
        on error (e.g., table does not exist yet).
        """
        try:
            logger.debug("SQL: %s", sql)
            return self.db.query(sql)
        except Exception as e:
            logger.warning("Query failed: %s — %s", sql[:120], e)
            return pd.DataFrame()


def _shift_year(date_str: str, years: int) -> str:
    """Shift a YYYY-MM-DD date string by N years."""
    if not date_str:
        return date_str
    try:
        parts = date_str.split("-")
        new_year = int(parts[0]) + years
        # Handle Feb 29 edge case
        month, day = int(parts[1]), int(parts[2])
        if month == 2 and day == 29:
            # Check if target year is a leap year
            import calendar
            if not calendar.isleap(new_year):
                day = 28
        return f"{new_year:04d}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        return date_str


def _dimension_matches(cell_value, target: str) -> bool:
    """Check if a dimension value (which may be a JSON list, comma-separated,
    a single value, or 'ALL') matches the target.
    """
    import json as _json

    if pd.isna(cell_value):
        return True  # NULL means "all" / not filtered
    cell_str = str(cell_value).strip()
    if cell_str.upper() == "ALL" or '"ALL"' in cell_str:
        return True

    # Try JSON array first (e.g. '["US", "GB"]')
    if cell_str.startswith("["):
        try:
            values = _json.loads(cell_str)
            return target in values or "ALL" in values
        except _json.JSONDecodeError:
            pass

    # Fall back to comma-separated
    values = [v.strip().strip('"').strip("'") for v in cell_str.split(",")]
    return target in values
