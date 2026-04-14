"""Stage 3: Enrich parsed query and fetched data with domain knowledge."""

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class ContextEnricher:
    """Add metric definitions, seasonal baselines, and pre-computed insights
    to the data before sending it to the attribution reasoner.
    """

    def __init__(self, data_dir: str = "data/synthetic"):
        self.data_dir = Path(data_dir)
        self._load_knowledge()

    def _load_knowledge(self):
        """Load metric definitions and seasonal patterns from JSON files."""
        metric_defs_path = self.data_dir / "metric_definitions.json"
        if metric_defs_path.exists():
            self.metric_defs = json.loads(metric_defs_path.read_text())
        else:
            logger.info("No metric_definitions.json found at %s, using empty", metric_defs_path)
            self.metric_defs = {}

        seasonal_path = self.data_dir / "seasonal_patterns.json"
        if seasonal_path.exists():
            self.seasonal_patterns = json.loads(seasonal_path.read_text())
        else:
            logger.info("No seasonal_patterns.json found at %s, using empty", seasonal_path)
            self.seasonal_patterns = {}

    def enrich(self, parsed_query: dict, fetched_data: dict) -> dict:
        """Add context: metric definition, seasonal baseline, pre-computed insights.

        Returns dict with keys:
            metric_definition, seasonal_context, initiative_details,
            confounder_details, movement_summary, parsed_query, fetched_data_tables.
        """
        metric = parsed_query.get("metric", "")
        market = parsed_query.get("market", "ALL")
        period = parsed_query.get("period", {})

        metric_definition = self._get_metric_definition(metric)
        seasonal_context = self._get_seasonal_context(metric, market, period)
        initiative_details = self._format_initiatives(fetched_data.get("initiatives"))
        confounder_details = self._format_confounders(fetched_data.get("confounders"))
        movement_summary = self._compute_movement_summary(
            parsed_query, fetched_data.get("primary_data")
        )
        adjacent_summary = self._summarize_adjacent_metrics(
            fetched_data.get("adjacent_metrics")
        )
        cross_market_summary = self._summarize_cross_market(
            fetched_data.get("cross_market"), metric
        )
        change_point_summary = self._format_change_points(
            fetched_data.get("change_points")
        )
        fetched_data_tables = self._format_data_tables(fetched_data)

        return {
            "metric": metric,
            "market": market,
            "category": parsed_query.get("category", "ALL"),
            "segment": parsed_query.get("segment", "ALL"),
            "period": period,
            "direction": parsed_query.get("direction"),
            "magnitude": parsed_query.get("magnitude"),
            "metric_definition": metric_definition,
            "seasonal_context": seasonal_context,
            "initiative_details": initiative_details,
            "confounder_details": confounder_details,
            "movement_summary": movement_summary,
            "adjacent_metrics_summary": adjacent_summary,
            "cross_market_summary": cross_market_summary,
            "change_point_summary": change_point_summary,
            "fetched_data_tables": fetched_data_tables,
            "parsed_query": parsed_query,
        }

    # ------------------------------------------------------------------
    # Metric definition
    # ------------------------------------------------------------------

    def _get_metric_definition(self, metric: str) -> str:
        """Return a human-readable metric definition string."""
        if metric in self.metric_defs:
            defn = self.metric_defs[metric]
            if isinstance(defn, dict):
                parts = []
                if "name" in defn:
                    parts.append(f"**{defn['name']}**")
                if "definition" in defn:
                    parts.append(defn["definition"])
                if "business_logic" in defn:
                    parts.append(f"Business logic: {defn['business_logic']}")
                if "unit" in defn:
                    parts.append(f"Unit: {defn['unit']}")
                if "caveats" in defn:
                    parts.append(f"Caveats: {defn['caveats']}")
                return "\n".join(parts)
            return str(defn)
        return f"No definition found for metric '{metric}'."

    # ------------------------------------------------------------------
    # Seasonal context
    # ------------------------------------------------------------------

    def _get_seasonal_context(self, metric: str, market: str, period: dict) -> str:
        """Look up seasonal baselines for this metric x market x period."""
        if not self.seasonal_patterns:
            return "No seasonal pattern data available."

        # Try metric-specific patterns
        metric_patterns = self.seasonal_patterns.get(metric, {})
        if not metric_patterns:
            # Try global patterns
            metric_patterns = self.seasonal_patterns.get("_global", {})

        if not metric_patterns:
            return "No seasonal pattern data available for this metric."

        # Build context from available pattern info
        parts = []

        # Market-specific seasonality
        market_patterns = metric_patterns.get(market, metric_patterns.get("ALL", {}))
        if isinstance(market_patterns, dict):
            for key, value in market_patterns.items():
                parts.append(f"- {key}: {value}")
        elif isinstance(market_patterns, str):
            parts.append(market_patterns)
        elif isinstance(market_patterns, list):
            for item in market_patterns:
                parts.append(f"- {item}")

        # Check for period-specific notes (e.g., holiday effects)
        start_date = period.get("start_date", "")
        if start_date:
            month = start_date.split("-")[1] if len(start_date) >= 7 else ""
            monthly_notes = metric_patterns.get(f"month_{month}", "")
            if monthly_notes:
                parts.append(f"Period-specific note: {monthly_notes}")

        if not parts:
            return "No seasonal pattern data available for this metric/market/period."

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Initiative formatting
    # ------------------------------------------------------------------

    def _format_initiatives(self, initiatives_df: pd.DataFrame | None) -> str:
        """Format the initiatives DataFrame into a readable string for the prompt."""
        if initiatives_df is None or initiatives_df.empty:
            return "No active initiatives found during this period."

        lines = []
        for _, row in initiatives_df.iterrows():
            parts = []
            if "initiative_id" in row:
                parts.append(f"ID: {row['initiative_id']}")
            if "name" in row:
                parts.append(f"Name: {row['name']}")
            if "description" in row:
                parts.append(f"Description: {row['description']}")
            if "start_date" in row:
                parts.append(f"Start: {row['start_date']}")
            if "end_date" in row:
                parts.append(f"End: {row['end_date']}")
            if "type" in row:
                parts.append(f"Type: {row['type']}")
            if "expected_impact" in row:
                parts.append(f"Expected impact: {row['expected_impact']}")
            if "target_metric" in row:
                parts.append(f"Target metric: {row['target_metric']}")
            if "market_id" in row:
                parts.append(f"Market: {row['market_id']}")
            if "category_id" in row:
                parts.append(f"Category: {row['category_id']}")
            if "segment_id" in row:
                parts.append(f"Segment: {row['segment_id']}")

            lines.append(" | ".join(parts))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Confounder formatting
    # ------------------------------------------------------------------

    def _format_confounders(self, confounders_df: pd.DataFrame | None) -> str:
        """Format the confounders DataFrame into a readable string."""
        if confounders_df is None or confounders_df.empty:
            return "No known confounders during this period."

        lines = []
        for _, row in confounders_df.iterrows():
            parts = []
            if "confounder_id" in row:
                parts.append(f"ID: {row['confounder_id']}")
            if "name" in row:
                parts.append(f"Name: {row['name']}")
            if "description" in row:
                parts.append(f"Description: {row['description']}")
            if "type" in row:
                parts.append(f"Type: {row['type']}")
            if "start_date" in row:
                parts.append(f"Start: {row['start_date']}")
            if "end_date" in row:
                parts.append(f"End: {row['end_date']}")
            if "impact_direction" in row:
                parts.append(f"Impact direction: {row['impact_direction']}")
            if "impact_magnitude" in row:
                parts.append(f"Impact magnitude: {row['impact_magnitude']}")
            if "affected_metrics" in row:
                parts.append(f"Affected metrics: {row['affected_metrics']}")
            if "market_id" in row:
                parts.append(f"Market: {row['market_id']}")

            lines.append(" | ".join(parts))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Movement summary
    # ------------------------------------------------------------------

    def _compute_movement_summary(
        self, parsed_query: dict, primary_data: pd.DataFrame | None
    ) -> str:
        """Compute a human-readable summary of the metric movement."""
        metric = parsed_query.get("metric", "unknown")
        market = parsed_query.get("market", "ALL")
        direction = parsed_query.get("direction", "unknown")
        magnitude = parsed_query.get("magnitude")
        period = parsed_query.get("period", {})

        summary_parts = [
            f"Metric: {metric}",
            f"Market: {market}",
            f"Period: {period.get('description', period.get('start_date', '?'))} "
            f"({period.get('start_date', '?')} to {period.get('end_date', '?')})",
        ]

        if direction:
            summary_parts.append(f"Direction: {direction}")
        if magnitude is not None:
            summary_parts.append(f"Reported magnitude: {magnitude}%")

        # Compute actual movement from data if available
        if primary_data is not None and not primary_data.empty and "value" in primary_data.columns:
            current = primary_data[
                primary_data.get("period_label", pd.Series(dtype=str)) == "current"
            ] if "period_label" in primary_data.columns else primary_data

            if not current.empty:
                values = current["value"].dropna()
                if len(values) >= 2:
                    first_half = values.iloc[: len(values) // 2].mean()
                    second_half = values.iloc[len(values) // 2 :].mean()
                    if first_half != 0:
                        pct_change = ((second_half - first_half) / abs(first_half)) * 100
                        computed_direction = "increase" if pct_change > 0 else "decrease"
                        summary_parts.append(
                            f"Computed movement: {computed_direction} of "
                            f"{abs(pct_change):.1f}% (first half avg: {first_half:.4f}, "
                            f"second half avg: {second_half:.4f})"
                        )

                # Add overall stats
                summary_parts.append(
                    f"Period mean: {values.mean():.4f}, "
                    f"min: {values.min():.4f}, max: {values.max():.4f}, "
                    f"std: {values.std():.4f}"
                )

            # YoY comparison
            if "period_label" in primary_data.columns:
                yoy = primary_data[primary_data["period_label"] == "yoy"]
                if not yoy.empty and "value" in yoy.columns:
                    yoy_mean = yoy["value"].dropna().mean()
                    current_mean = current["value"].dropna().mean() if not current.empty else 0
                    if yoy_mean != 0:
                        yoy_change = ((current_mean - yoy_mean) / abs(yoy_mean)) * 100
                        summary_parts.append(
                            f"Year-over-year change: {yoy_change:+.1f}% "
                            f"(current avg: {current_mean:.4f}, YoY avg: {yoy_mean:.4f})"
                        )

        return "\n".join(summary_parts)

    # ------------------------------------------------------------------
    # Adjacent metric summary
    # ------------------------------------------------------------------

    def _summarize_adjacent_metrics(
        self, adjacent_df: pd.DataFrame | None
    ) -> str:
        """Summarize trends in adjacent metrics."""
        if adjacent_df is None or adjacent_df.empty:
            return "No adjacent metric data available."

        lines = []
        if "metric_name" not in adjacent_df.columns or "value" not in adjacent_df.columns:
            return "Adjacent metrics data has unexpected schema."

        for metric_name, group in adjacent_df.groupby("metric_name"):
            values = group["value"].dropna()
            if len(values) < 2:
                lines.append(f"- {metric_name}: insufficient data points")
                continue

            first_half = values.iloc[: len(values) // 2].mean()
            second_half = values.iloc[len(values) // 2 :].mean()
            if first_half != 0:
                pct_change = ((second_half - first_half) / abs(first_half)) * 100
                direction = "up" if pct_change > 0 else "down"
                lines.append(
                    f"- {metric_name}: {direction} {abs(pct_change):.1f}% "
                    f"(mean: {values.mean():.4f})"
                )
            else:
                lines.append(f"- {metric_name}: mean = {values.mean():.4f}")

        return "\n".join(lines) if lines else "No adjacent metric trends computed."

    # ------------------------------------------------------------------
    # Cross-market summary
    # ------------------------------------------------------------------

    def _summarize_cross_market(
        self, cross_market_df: pd.DataFrame | None, metric: str
    ) -> str:
        """Summarize the same metric across other markets."""
        if cross_market_df is None or cross_market_df.empty:
            return "No cross-market comparison data available."

        if "market_id" not in cross_market_df.columns or "value" not in cross_market_df.columns:
            return "Cross-market data has unexpected schema."

        lines = []
        for market_id, group in cross_market_df.groupby("market_id"):
            values = group["value"].dropna()
            if len(values) < 2:
                lines.append(f"- {market_id}: insufficient data")
                continue

            first_half = values.iloc[: len(values) // 2].mean()
            second_half = values.iloc[len(values) // 2 :].mean()
            if first_half != 0:
                pct_change = ((second_half - first_half) / abs(first_half)) * 100
                direction = "up" if pct_change > 0 else "down"
                lines.append(
                    f"- {market_id}: {direction} {abs(pct_change):.1f}% "
                    f"(mean: {values.mean():.4f})"
                )
            else:
                lines.append(f"- {market_id}: mean = {values.mean():.4f}")

        return "\n".join(lines) if lines else "No cross-market trends computed."

    # ------------------------------------------------------------------
    # Change point formatting
    # ------------------------------------------------------------------

    def _format_change_points(self, change_points_df: pd.DataFrame | None) -> str:
        """Format detected change points into a readable string."""
        if change_points_df is None or change_points_df.empty:
            return "No change points detected near this period."

        lines = []
        for _, row in change_points_df.iterrows():
            parts = []
            if "date" in row:
                parts.append(f"Date: {row['date']}")
            if "metric_name" in row:
                parts.append(f"Metric: {row['metric_name']}")
            if "direction" in row:
                parts.append(f"Direction: {row['direction']}")
            if "magnitude" in row:
                parts.append(f"Magnitude: {row['magnitude']}")
            if "confidence" in row:
                parts.append(f"Confidence: {row['confidence']}")
            if "description" in row:
                parts.append(f"Note: {row['description']}")

            lines.append(" | ".join(parts))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Data table formatting for LLM prompt
    # ------------------------------------------------------------------

    def _format_data_tables(self, fetched_data: dict) -> str:
        """Convert fetched DataFrames into markdown tables for the LLM prompt."""
        sections = []

        for key in ["primary_data", "adjacent_metrics", "cross_market"]:
            df = fetched_data.get(key)
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                continue

            # Limit rows to avoid prompt bloat
            display_df = df.head(100)
            table_str = display_df.to_markdown(index=False)
            sections.append(f"### {key.replace('_', ' ').title()}\n{table_str}")

        return "\n\n".join(sections) if sections else "No data tables available."
