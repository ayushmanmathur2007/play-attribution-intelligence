"""Stage 1: Parse natural language queries into structured parameters."""

import json
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# Country name to market_id mapping for regex fallback
COUNTRY_TO_MARKET = {
    "united states": "US", "usa": "US", "us": "US", "america": "US",
    "united kingdom": "UK", "uk": "GB", "britain": "GB", "england": "GB",
    "germany": "DE", "deutschland": "DE",
    "japan": "JP",
    "south korea": "KR", "korea": "KR",
    "brazil": "BR", "brasil": "BR",
    "india": "IN",
    "mexico": "MX",
    "indonesia": "ID",
    "turkey": "TR",
    "russia": "RU",
    "nigeria": "NG",
    "philippines": "PH",
    "egypt": "EG",
    "vietnam": "VN",
}

# Informal metric name aliases for regex fallback
METRIC_ALIASES = {
    "offer clicks": "offer_ctr",
    "click through rate": "offer_ctr",
    "click-through rate": "offer_ctr",
    "ctr": "offer_ctr",
    "offer ctr": "offer_ctr",
    "redemption rate": "offer_redemption_rate",
    "redemptions": "offer_redemption_count",
    "redemption count": "offer_redemption_count",
    "offer revenue": "offer_driven_revenue",
    "offer-driven revenue": "offer_driven_revenue",
    "revenue from offers": "offer_driven_revenue",
    "offer cost": "offer_cost",
    "offer roi": "offer_roi",
    "return on investment": "offer_roi",
    "impressions": "offer_impression_count",
    "offer impressions": "offer_impression_count",
    "time to redemption": "avg_time_to_redemption",
    "daily active users": "dau",
    "dau": "dau",
    "weekly active users": "wau",
    "wau": "wau",
    "day 7 retention": "d7_retention",
    "d7 retention": "d7_retention",
    "7-day retention": "d7_retention",
    "day 30 retention": "d30_retention",
    "d30 retention": "d30_retention",
    "30-day retention": "d30_retention",
    "arpu": "revenue_per_user",
    "revenue per user": "revenue_per_user",
    "ltv": "ltv_30d",
    "lifetime value": "ltv_30d",
    "sessions per user": "sessions_per_user",
    "session duration": "avg_session_duration",
    "play points earn": "play_points_earn_rate",
    "play points burn": "play_points_burn_rate",
    "points balance": "play_points_balance_avg",
    "loyalty purchases": "loyalty_driven_purchases",
    "store conversion": "store_visit_to_install_rate",
    "install rate": "store_visit_to_install_rate",
    "first purchase rate": "install_to_first_purchase_rate",
    "funnel conversion": "offer_funnel_conversion",
    "subscription conversion": "subscription_conversion_rate",
    "churn": "churn_rate",
    "churn rate": "churn_rate",
}


class QueryParser:
    """Parse natural language attribution queries into structured parameters."""

    def __init__(self, llm_client, config: dict):
        self.llm = llm_client
        self.prompt_template = (
            Path(__file__).parent / "prompts" / "query_parser.txt"
        ).read_text()
        self.config = config
        self._build_lookup_tables()

    def _build_lookup_tables(self):
        """Build fast lookup structures from the dimensions config."""
        self.metric_ids = set()
        self.metric_names = {}  # id -> name
        for m in self.config.get("metrics", []):
            mid = m["id"]
            self.metric_ids.add(mid)
            self.metric_names[mid] = m.get("name", mid)

        self.market_ids = set()
        self.market_names = {}  # id -> name
        for m in self.config.get("markets", []):
            mid = m["id"]
            self.market_ids.add(mid)
            self.market_names[mid] = m.get("name", mid)

        self.category_ids = {c["id"] for c in self.config.get("categories", [])}
        self.segment_ids = {s["id"] for s in self.config.get("segments", [])}

    async def parse(self, query: str) -> dict:
        """Parse natural language query into structured params.

        Returns dict with keys: metric, market, category, segment,
        period (start_date, end_date, description), direction, magnitude,
        original_query.
        """
        try:
            result = await self._llm_parse(query)
            validated = self._validate(result)
            return validated
        except Exception as e:
            logger.warning("LLM parsing failed (%s), falling back to regex", e)
            return self._regex_fallback(query)

    async def _llm_parse(self, query: str) -> dict:
        """Use LLM to parse the query."""
        today = datetime.utcnow().strftime("%Y-%m-%d")

        metric_list = "\n".join(
            f"- {m['id']}: {m.get('name', m['id'])}"
            for m in self.config.get("metrics", [])
        )
        market_list = "\n".join(
            f"- {m['id']}: {m.get('name', m['id'])}"
            for m in self.config.get("markets", [])
        )
        category_list = "\n".join(
            f"- {c['id']}: {c.get('name', c['id'])}"
            for c in self.config.get("categories", [])
        )
        segment_list = "\n".join(
            f"- {s['id']}: {s.get('name', s['id'])}"
            for s in self.config.get("segments", [])
        )

        system_prompt = self.prompt_template
        for placeholder, value in {
            "{metric_list}": metric_list,
            "{market_list}": market_list,
            "{category_list}": category_list,
            "{segment_list}": segment_list,
            "{current_date}": today,
        }.items():
            system_prompt = system_prompt.replace(placeholder, str(value))

        response = await self.llm.complete(
            system_prompt=system_prompt,
            user_prompt=query,
            temperature=0.1,
            max_tokens=1024,
            response_format="json",
        )

        # Strip markdown fences if present
        text = response.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        return json.loads(text)

    def _validate(self, parsed: dict) -> dict:
        """Validate and normalize the LLM output."""
        metric = parsed.get("metric", "")
        if metric and metric not in self.metric_ids:
            # Try to find the closest match
            closest = self._fuzzy_match_metric(metric)
            if closest:
                parsed["metric"] = closest
            else:
                logger.warning("Unknown metric '%s' from LLM, keeping as-is", metric)

        market = parsed.get("market", "ALL")
        if market != "ALL" and market not in self.market_ids:
            closest = self._fuzzy_match_market(market)
            if closest:
                parsed["market"] = closest

        category = parsed.get("category", "ALL")
        if category != "ALL" and category not in self.category_ids:
            parsed["category"] = "ALL"

        segment = parsed.get("segment", "ALL")
        if segment != "ALL" and segment not in self.segment_ids:
            parsed["segment"] = "ALL"

        # Ensure period exists with valid dates
        period = parsed.get("period", {})
        if not period or not period.get("start_date"):
            today = datetime.utcnow()
            period = {
                "start_date": (today - timedelta(days=28)).strftime("%Y-%m-%d"),
                "end_date": today.strftime("%Y-%m-%d"),
                "description": "last 4 weeks (default)",
            }
            parsed["period"] = period

        # Ensure direction is normalized
        direction = parsed.get("direction")
        if direction and direction.lower() not in ("increase", "decrease"):
            parsed["direction"] = None

        # Ensure magnitude is numeric or None
        mag = parsed.get("magnitude")
        if mag is not None:
            try:
                parsed["magnitude"] = float(mag)
            except (ValueError, TypeError):
                parsed["magnitude"] = None

        parsed.setdefault("original_query", "")
        parsed.setdefault("market", "ALL")
        parsed.setdefault("category", "ALL")
        parsed.setdefault("segment", "ALL")
        parsed.setdefault("direction", None)
        parsed.setdefault("magnitude", None)

        return parsed

    def _fuzzy_match_metric(self, candidate: str) -> str | None:
        """Try to match a metric name loosely."""
        candidate_lower = candidate.lower().replace("-", "_").replace(" ", "_")
        for mid in self.metric_ids:
            if candidate_lower == mid.lower():
                return mid
            if candidate_lower in mid.lower() or mid.lower() in candidate_lower:
                return mid
        return None

    def _fuzzy_match_market(self, candidate: str) -> str | None:
        """Try to match a market by name or id."""
        candidate_lower = candidate.lower().strip()
        # Direct ID match
        candidate_upper = candidate.upper().strip()
        if candidate_upper in self.market_ids:
            return candidate_upper
        # Country name match
        if candidate_lower in COUNTRY_TO_MARKET:
            return COUNTRY_TO_MARKET[candidate_lower]
        # Partial name match
        for mid, name in self.market_names.items():
            if candidate_lower in name.lower():
                return mid
        return None

    def _regex_fallback(self, query: str) -> dict:
        """Best-effort extraction using regex when LLM fails."""
        query_lower = query.lower()

        # Extract metric
        metric = self._extract_metric_regex(query_lower)

        # Extract market
        market = self._extract_market_regex(query_lower)

        # Extract direction
        direction = None
        if any(w in query_lower for w in ("increase", "up", "rose", "grew", "spike", "jump", "surge")):
            direction = "increase"
        elif any(w in query_lower for w in ("decrease", "down", "drop", "fell", "decline", "dip", "plummet")):
            direction = "decrease"

        # Extract magnitude (look for percentages)
        magnitude = None
        pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", query)
        if pct_match:
            magnitude = float(pct_match.group(1))

        # Extract period
        period = self._extract_period_regex(query_lower)

        return {
            "metric": metric,
            "market": market,
            "category": "ALL",
            "segment": "ALL",
            "period": period,
            "direction": direction,
            "magnitude": magnitude,
            "original_query": query,
        }

    def _extract_metric_regex(self, query_lower: str) -> str:
        """Try to find a metric name in the query text."""
        # Check aliases first (longest match first to avoid partial hits)
        sorted_aliases = sorted(METRIC_ALIASES.keys(), key=len, reverse=True)
        for alias in sorted_aliases:
            if alias in query_lower:
                return METRIC_ALIASES[alias]

        # Check raw metric IDs (with underscores replaced by spaces)
        for mid in self.metric_ids:
            readable = mid.replace("_", " ")
            if readable in query_lower or mid in query_lower:
                return mid

        # Default to a common metric
        return "offer_redemption_rate"

    def _extract_market_regex(self, query_lower: str) -> str:
        """Try to find a market reference in the query text."""
        # Check country names (longest first)
        sorted_countries = sorted(COUNTRY_TO_MARKET.keys(), key=len, reverse=True)
        for country in sorted_countries:
            if country in query_lower:
                return COUNTRY_TO_MARKET[country]

        # Check market IDs directly (e.g., "in US" or "for JP")
        for mid in self.market_ids:
            # Look for the market ID as a standalone word
            if re.search(rf"\b{mid}\b", query_lower, re.IGNORECASE):
                return mid

        return "ALL"

    def _extract_period_regex(self, query_lower: str) -> dict:
        """Try to extract a time period from the query text."""
        today = datetime.utcnow()

        if "last week" in query_lower:
            # Last full calendar week (Mon-Sun)
            days_since_monday = today.weekday()
            last_monday = today - timedelta(days=days_since_monday + 7)
            last_sunday = last_monday + timedelta(days=6)
            return {
                "start_date": last_monday.strftime("%Y-%m-%d"),
                "end_date": last_sunday.strftime("%Y-%m-%d"),
                "description": "last week",
            }

        if "last month" in query_lower:
            first_of_this_month = today.replace(day=1)
            last_day_prev_month = first_of_this_month - timedelta(days=1)
            first_of_prev_month = last_day_prev_month.replace(day=1)
            return {
                "start_date": first_of_prev_month.strftime("%Y-%m-%d"),
                "end_date": last_day_prev_month.strftime("%Y-%m-%d"),
                "description": "last month",
            }

        if "last 2 weeks" in query_lower or "past 2 weeks" in query_lower:
            return {
                "start_date": (today - timedelta(days=14)).strftime("%Y-%m-%d"),
                "end_date": today.strftime("%Y-%m-%d"),
                "description": "last 2 weeks",
            }

        if "yesterday" in query_lower:
            yesterday = today - timedelta(days=1)
            return {
                "start_date": yesterday.strftime("%Y-%m-%d"),
                "end_date": yesterday.strftime("%Y-%m-%d"),
                "description": "yesterday",
            }

        # Look for explicit date ranges (YYYY-MM-DD to YYYY-MM-DD)
        date_range = re.search(
            r"(\d{4}-\d{2}-\d{2})\s+(?:to|through|until|-)\s+(\d{4}-\d{2}-\d{2})",
            query_lower,
        )
        if date_range:
            return {
                "start_date": date_range.group(1),
                "end_date": date_range.group(2),
                "description": f"{date_range.group(1)} to {date_range.group(2)}",
            }

        # Check for "last N days" pattern
        n_days = re.search(r"(?:last|past)\s+(\d+)\s+days?", query_lower)
        if n_days:
            n = int(n_days.group(1))
            return {
                "start_date": (today - timedelta(days=n)).strftime("%Y-%m-%d"),
                "end_date": today.strftime("%Y-%m-%d"),
                "description": f"last {n} days",
            }

        # Default: last 4 weeks
        return {
            "start_date": (today - timedelta(days=28)).strftime("%Y-%m-%d"),
            "end_date": today.strftime("%Y-%m-%d"),
            "description": "last 4 weeks (default)",
        }
