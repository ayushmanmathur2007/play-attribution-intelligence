"""
Synthetic data generator for Google Play Loyalty & Offers metrics.

Generates 18 months of daily data across 15 markets x 12 categories x N segments
x M metrics. Supports both a sampled mode (~3M rows) for fast iteration and a full
mode (~25M rows) for comprehensive analysis.

Value formula:
    final_value = (base_value
                  * market_modifier
                  * category_modifier
                  * segment_modifier
                  * structural_trend(date)
                  * seasonality(date, market, category)
                  + sum(initiative_impacts)
                  + sum(confounder_impacts)
                  + noise(sigma = base_value * 0.02))
"""

import json
import logging
import time
import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

from .seasonality import compute_seasonality
from .trends import compute_structural_trend
from .initiatives import get_initiatives
from .confounders import get_confounders
from .golden_dataset import detect_movements

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Segments used in sampled mode to keep row count under ~3M
SAMPLED_SEGMENTS = {"ACT_HIGH", "ACT_MED", "ACT_FREE"}

# Units that represent absolute/additive quantities
ADDITIVE_UNITS = {"count", "usd", "points", "hours", "minutes"}

# Units that represent rates or ratios (multiplicative initiative impact)
MULTIPLICATIVE_UNITS = {"rate", "ratio"}

# Category modifiers: vertical -> metric_unit -> modifier
# Games categories get a boost on engagement; Apps get a boost on revenue
CATEGORY_MODIFIERS: dict[str, dict[str, float]] = {
    "Games": {
        "count": 1.2,    # higher engagement / session counts
        "rate": 1.05,    # slightly higher conversion rates
        "usd": 0.9,      # lower per-user revenue than apps
        "ratio": 1.0,
        "points": 1.15,  # more play points activity in games
        "hours": 0.8,    # faster redemption in games
        "minutes": 1.3,  # longer sessions in games
    },
    "Apps": {
        "count": 0.9,
        "rate": 1.0,
        "usd": 1.1,      # higher per-user revenue
        "ratio": 1.05,
        "points": 0.85,
        "hours": 1.1,
        "minutes": 0.8,
    },
}

# Per-category fine-grained multipliers (layered on top of vertical modifier)
CATEGORY_SPECIFIC_MODIFIERS: dict[str, float] = {
    "GAM_CAS": 1.15,    # casual games have widest reach
    "GAM_MID": 0.95,
    "GAM_HC": 0.75,     # smaller but higher-value audience
    "GAM_HYP": 1.25,    # massive volume, low value
    "APP_SOC": 1.10,
    "APP_ENT": 1.05,
    "APP_PRD": 0.85,
    "APP_FIN": 0.80,
    "APP_SHP": 1.20,    # shopping has high offer engagement
    "APP_EDU": 0.70,
    "APP_HLT": 0.75,
    "APP_TRV": 0.90,
}

# Journey archetype names
JOURNEY_ARCHETYPES = ["discovery", "engage", "monetize", "loyalty", "churn"]


# ---------------------------------------------------------------------------
# Helper: trapezoidal envelope for initiative / confounder impact
# ---------------------------------------------------------------------------

def _trapezoidal_envelope(
    date: datetime,
    start: datetime,
    end: datetime,
    ramp_up_days: int,
    decay_days: int,
) -> float:
    """Return a value in [0, 1] representing the trapezoidal impact envelope.

    Timeline:
        [start ... start+ramp_up]  linear ramp 0->1
        [start+ramp_up ... end]    full effect = 1.0
        [end ... end+decay]        linear decay 1->0
        outside                    0.0
    """
    if date < start or (decay_days > 0 and date > end + timedelta(days=decay_days)):
        return 0.0
    if decay_days == 0 and date > end:
        return 0.0

    # Ramp-up phase
    if ramp_up_days > 0 and date < start + timedelta(days=ramp_up_days):
        return (date - start).days / ramp_up_days

    # Full effect phase
    if date <= end:
        return 1.0

    # Decay phase
    if decay_days > 0:
        elapsed_after_end = (date - end).days
        return max(0.0, 1.0 - elapsed_after_end / decay_days)

    return 0.0


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class DataGenerator:
    """Generates synthetic Google Play Loyalty & Offers data.

    Parameters
    ----------
    config_path : str
        Path to dimensions.yaml relative to project root.
    output_dir : str
        Directory for generated artefacts (relative to project root).
    months : int
        Number of months of history to generate (default 18).
    full : bool
        If False (default), sample 3 key segments per combination to keep
        row count manageable (~3M). If True, use all 10 segments (~25M).
    seed : int
        Random seed for reproducibility.
    """

    def __init__(
        self,
        config_path: str = "config/dimensions.yaml",
        output_dir: str = "data/synthetic",
        months: int = 18,
        full: bool = False,
        seed: int = 42,
    ):
        self._project_root = Path(__file__).resolve().parents[2]
        self._config_path = self._project_root / config_path
        self._output_dir = self._project_root / output_dir
        self._months = months
        self._full = full
        self._seed = seed
        self._rng = np.random.default_rng(seed)

        # Loaded in _load_config
        self._markets: list[dict[str, Any]] = []
        self._categories: list[dict[str, Any]] = []
        self._segments: list[dict[str, Any]] = []
        self._metrics: list[dict[str, Any]] = []
        self._journey_archetypes: list[str] = JOURNEY_ARCHETYPES

        # Loaded from sibling modules
        self._initiatives: list[dict[str, Any]] = []
        self._confounders: list[dict[str, Any]] = []

        # Lookup caches built during _load_config
        self._market_by_id: dict[str, dict[str, Any]] = {}
        self._category_by_id: dict[str, dict[str, Any]] = {}
        self._segment_by_id: dict[str, dict[str, Any]] = {}
        self._metric_by_id: dict[str, dict[str, Any]] = {}

        # Date range
        self._end_date = datetime(2026, 3, 31)
        self._start_date = self._end_date - timedelta(days=self._months * 30)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> None:
        """Orchestrate the full generation pipeline."""
        t0 = time.time()
        logger.info("Starting synthetic data generation (full=%s)", self._full)

        self._load_config()
        self._load_external_definitions()

        logger.info("Generating daily metrics...")
        daily_df = self._generate_daily_metrics()
        logger.info(
            "Daily metrics: %s rows, %.1f MB",
            len(daily_df),
            daily_df.memory_usage(deep=True).sum() / 1e6,
        )

        logger.info("Generating journey aggregates...")
        journey_df = self._generate_journey_aggregates()
        logger.info("Journey aggregates: %s rows", len(journey_df))

        logger.info("Exporting all artefacts...")
        self._export_data(daily_df, journey_df)

        elapsed = time.time() - t0
        logger.info("Data generation complete in %.1fs", elapsed)

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _load_config(self) -> None:
        """Load dimensions from YAML and build lookup dicts."""
        with open(self._config_path, "r") as f:
            cfg = yaml.safe_load(f)

        self._markets = cfg["markets"]
        self._categories = cfg["categories"]
        self._segments = cfg["segments"]
        self._metrics = cfg["metrics"]

        if "journey_archetypes" in cfg:
            self._journey_archetypes = cfg["journey_archetypes"]

        # Build lookup dicts
        self._market_by_id = {m["id"]: m for m in self._markets}
        self._category_by_id = {c["id"]: c for c in self._categories}
        self._segment_by_id = {s["id"]: s for s in self._segments}
        self._metric_by_id = {m["id"]: m for m in self._metrics}

        logger.info(
            "Loaded %d markets, %d categories, %d segments, %d metrics",
            len(self._markets),
            len(self._categories),
            len(self._segments),
            len(self._metrics),
        )

    def _load_external_definitions(self) -> None:
        """Load initiatives and confounders from sibling modules."""
        self._initiatives = get_initiatives()
        self._confounders = get_confounders()
        logger.info(
            "Loaded %d initiatives, %d confounders",
            len(self._initiatives),
            len(self._confounders),
        )

    # ------------------------------------------------------------------
    # Core generation: daily metrics
    # ------------------------------------------------------------------

    def _generate_daily_metrics(self) -> pd.DataFrame:
        """Generate daily metric values for all dimension combinations.

        To keep memory manageable, we iterate per-metric and build the
        DataFrame in chunks, then concatenate at the end.
        """
        dates = pd.date_range(self._start_date, self._end_date, freq="D")
        num_days = len(dates)

        # Choose segments
        if self._full:
            segments = self._segments
        else:
            segments = [s for s in self._segments if s["id"] in SAMPLED_SEGMENTS]

        market_ids = [m["id"] for m in self._markets]
        category_ids = [c["id"] for c in self._categories]
        segment_ids = [s["id"] for s in segments]

        num_markets = len(market_ids)
        num_categories = len(category_ids)
        num_segments = len(segment_ids)

        logger.info(
            "Generating %d days x %d markets x %d categories x %d segments x %d metrics",
            num_days,
            num_markets,
            num_categories,
            num_segments,
            len(self._metrics),
        )

        all_chunks: list[pd.DataFrame] = []

        for metric in self._metrics:
            metric_id = metric["id"]
            metric_unit = metric["unit"]
            base_values = metric["base_values"]

            # Pre-compute per-market base values and modifiers as arrays
            # Shape: (num_markets,)
            market_bases = np.array(
                [self._get_market_base(m, base_values) for m in self._markets],
                dtype=np.float64,
            )
            market_modifiers = np.array(
                [self._get_market_modifier(m, metric_unit) for m in self._markets],
                dtype=np.float64,
            )

            # Pre-compute per-category modifiers: (num_categories,)
            cat_modifiers = np.array(
                [self._get_category_modifier(c, metric_unit) for c in self._categories],
                dtype=np.float64,
            )

            # Pre-compute per-segment modifiers: (num_segments,)
            seg_modifiers = np.array(
                [self._get_segment_modifier(s, metric_unit) for s in segments],
                dtype=np.float64,
            )

            # Pre-compute structural trend for each (date, market): (num_days, num_markets)
            trends = np.empty((num_days, num_markets), dtype=np.float64)
            for di, d in enumerate(dates):
                dt = d.to_pydatetime()
                for mi, mkt in enumerate(self._markets):
                    trends[di, mi] = compute_structural_trend(dt, metric_id, mkt["id"])

            # Pre-compute seasonality for each (date, market, category): (num_days, num_markets, num_categories)
            seasonality = np.empty((num_days, num_markets, num_categories), dtype=np.float64)
            for di, d in enumerate(dates):
                dt = d.to_pydatetime()
                for mi, mkt in enumerate(self._markets):
                    for ci, cat in enumerate(self._categories):
                        seasonality[di, mi, ci] = compute_seasonality(dt, mkt["id"], cat["id"], metric_id)

            # Pre-compute initiative impacts per (date, market, category, segment):
            # We store as a dict keyed by (date_idx, market_idx, cat_idx, seg_idx) -> float
            # For efficiency, we only compute non-zero entries
            init_impacts = self._precompute_initiative_impacts(
                dates, market_ids, category_ids, segment_ids, metric_id, metric_unit,
            )

            # Pre-compute confounder impacts per (date, market, category):
            conf_impacts = self._precompute_confounder_impacts(
                dates, market_ids, category_ids, metric_id, metric_unit,
            )

            # Now vectorize across the full cartesian product
            # Total rows for this metric: num_days * num_markets * num_categories * num_segments
            total_rows = num_days * num_markets * num_categories * num_segments

            # Build index arrays using broadcasting
            # We use np.repeat/np.tile to expand dimensions
            date_idx = np.repeat(np.arange(num_days), num_markets * num_categories * num_segments)
            market_idx = np.tile(
                np.repeat(np.arange(num_markets), num_categories * num_segments),
                num_days,
            )
            cat_idx = np.tile(
                np.repeat(np.arange(num_categories), num_segments),
                num_days * num_markets,
            )
            seg_idx = np.tile(np.arange(num_segments), num_days * num_markets * num_categories)

            # Compute base signal: base_value * modifiers * trend * seasonality
            base_vals = market_bases[market_idx] * market_modifiers[market_idx]
            values = (
                base_vals
                * cat_modifiers[cat_idx]
                * seg_modifiers[seg_idx]
                * trends[date_idx, market_idx]
                * seasonality[date_idx, market_idx, cat_idx]
            )

            # Add initiative impacts
            init_arr = np.zeros(total_rows, dtype=np.float64)
            for (di, mi, ci, si), impact_val in init_impacts.items():
                flat_idx = (
                    di * num_markets * num_categories * num_segments
                    + mi * num_categories * num_segments
                    + ci * num_segments
                    + si
                )
                if metric_unit in MULTIPLICATIVE_UNITS:
                    # For rates/ratios, impact is multiplicative: value *= (1 + impact)
                    init_arr[flat_idx] += impact_val  # accumulate for multiplicative
                else:
                    init_arr[flat_idx] += impact_val

            if metric_unit in MULTIPLICATIVE_UNITS:
                values *= (1.0 + init_arr)
            else:
                values += init_arr

            # Add confounder impacts
            conf_arr = np.zeros(total_rows, dtype=np.float64)
            for (di, mi, ci), impact_val in conf_impacts.items():
                # Apply to all segments for this (date, market, category)
                start_flat = (
                    di * num_markets * num_categories * num_segments
                    + mi * num_categories * num_segments
                    + ci * num_segments
                )
                conf_arr[start_flat : start_flat + num_segments] += impact_val

            if metric_unit in MULTIPLICATIVE_UNITS:
                values *= (1.0 + conf_arr)
            else:
                values += conf_arr

            # Add noise
            noise_sigma = np.abs(base_vals) * 0.02
            noise = self._rng.normal(0, noise_sigma)
            values += noise

            # Clamp
            values = self._clamp_values(values, metric_unit)

            # Build DataFrame chunk
            chunk = pd.DataFrame({
                "date": dates[date_idx],
                "market_id": np.array(market_ids)[market_idx],
                "category_id": np.array(category_ids)[cat_idx],
                "segment_id": np.array(segment_ids)[seg_idx],
                "metric_name": metric_id,
                "value": values,
            })
            all_chunks.append(chunk)

            logger.debug("  Metric %s: %d rows generated", metric_id, total_rows)

        daily_df = pd.concat(all_chunks, ignore_index=True)

        # Optimize dtypes for memory
        daily_df["market_id"] = daily_df["market_id"].astype("category")
        daily_df["category_id"] = daily_df["category_id"].astype("category")
        daily_df["segment_id"] = daily_df["segment_id"].astype("category")
        daily_df["metric_name"] = daily_df["metric_name"].astype("category")
        daily_df["date"] = pd.to_datetime(daily_df["date"])

        return daily_df

    # ------------------------------------------------------------------
    # Modifier helpers
    # ------------------------------------------------------------------

    def _get_market_base(self, market: dict, base_values: dict) -> float:
        """Return the base value for a metric given the market tier."""
        tier_key = f"tier_{market['tier']}"
        return float(base_values.get(tier_key, base_values.get("tier_1", 0)))

    def _get_market_modifier(self, market: dict, metric_unit: str) -> float:
        """Return a scaling modifier based on market tier.

        For count/usd metrics, lower tiers get lower modifiers.
        For rate metrics, the base_values already encode the tier difference,
        so the modifier is close to 1.0.
        """
        tier = market["tier"]
        if metric_unit in MULTIPLICATIVE_UNITS:
            # Rates already differ by tier in base_values; apply a small modifier
            return {1: 1.0, 2: 1.0, 3: 1.0}[tier]
        else:
            # Count/USD metrics: the base_values per tier already capture the
            # main scaling, so we use 1.0 here. The tier difference is in base_values.
            return 1.0

    def _get_category_modifier(self, category: dict, metric_unit: str) -> float:
        """Return a modifier based on category vertical and specific category."""
        vertical = category["vertical"]
        vertical_mod = CATEGORY_MODIFIERS.get(vertical, {}).get(metric_unit, 1.0)
        specific_mod = CATEGORY_SPECIFIC_MODIFIERS.get(category["id"], 1.0)
        return vertical_mod * specific_mod

    def _get_segment_modifier(self, segment: dict, metric_unit: str) -> float:
        """Return a modifier based on segment.

        For count/volume metrics, scale by base_size_pct so smaller segments
        produce proportionally fewer events. For rate metrics, use 1.0
        (rates are intensive, not extensive).
        """
        if metric_unit in MULTIPLICATIVE_UNITS:
            return 1.0
        # For additive metrics, scale by relative segment size.
        # Normalize so the largest segment (ACT_MED at 0.25) maps to ~1.0
        return segment["base_size_pct"] / 0.25

    # ------------------------------------------------------------------
    # Initiative impact pre-computation
    # ------------------------------------------------------------------

    def _precompute_initiative_impacts(
        self,
        dates: pd.DatetimeIndex,
        market_ids: list[str],
        category_ids: list[str],
        segment_ids: list[str],
        metric_id: str,
        metric_unit: str,
    ) -> dict[tuple[int, int, int, int], float]:
        """Pre-compute the total initiative impact for each active cell.

        Returns a sparse dict: (date_idx, market_idx, cat_idx, seg_idx) -> impact.
        Only cells with non-zero impact are included.
        """
        impacts: dict[tuple[int, int, int, int], float] = {}
        market_idx_map = {mid: i for i, mid in enumerate(market_ids)}
        cat_idx_map = {cid: i for i, cid in enumerate(category_ids)}
        seg_idx_map = {sid: i for i, sid in enumerate(segment_ids)}

        for init in self._initiatives:
            # Check if this initiative affects the current metric
            init_impact_map = init.get("impact", {})
            if metric_id not in init_impact_map:
                continue

            magnitude = init_impact_map[metric_id]
            start = _parse_date(init["start_date"])
            end = _parse_date(init["end_date"])
            ramp_up = init.get("ramp_up_days", 3)
            decay = init.get("decay_days", 7)

            target_markets = init.get("target_markets", ["ALL"])
            target_segments = init.get("target_segments", ["ALL"])
            target_categories = init.get("target_categories", ["ALL"])

            # Determine which market/category/segment indices are targeted
            if "ALL" in target_markets:
                targeted_m_indices = list(range(len(market_ids)))
            else:
                targeted_m_indices = [
                    market_idx_map[m] for m in target_markets if m in market_idx_map
                ]

            if "ALL" in target_categories:
                targeted_c_indices = list(range(len(category_ids)))
            else:
                targeted_c_indices = [
                    cat_idx_map[c] for c in target_categories if c in cat_idx_map
                ]

            if "ALL" in target_segments:
                targeted_s_indices = list(range(len(segment_ids)))
            else:
                targeted_s_indices = [
                    seg_idx_map[s] for s in target_segments if s in seg_idx_map
                ]

            # Compute envelope for each date
            for di, d in enumerate(dates):
                dt = d.to_pydatetime()
                envelope = _trapezoidal_envelope(dt, start, end, ramp_up, decay)
                if envelope <= 0.0:
                    continue

                impact_val = magnitude * envelope

                for mi in targeted_m_indices:
                    for ci in targeted_c_indices:
                        for si in targeted_s_indices:
                            key = (di, mi, ci, si)
                            impacts[key] = impacts.get(key, 0.0) + impact_val

        return impacts

    # ------------------------------------------------------------------
    # Confounder impact pre-computation
    # ------------------------------------------------------------------

    def _precompute_confounder_impacts(
        self,
        dates: pd.DatetimeIndex,
        market_ids: list[str],
        category_ids: list[str],
        metric_id: str,
        metric_unit: str,
    ) -> dict[tuple[int, int, int], float]:
        """Pre-compute confounder impact for each (date, market, category).

        Returns a sparse dict: (date_idx, market_idx, cat_idx) -> impact.
        """
        impacts: dict[tuple[int, int, int], float] = {}
        market_idx_map = {mid: i for i, mid in enumerate(market_ids)}
        cat_idx_map = {cid: i for i, cid in enumerate(category_ids)}

        for conf in self._confounders:
            conf_impact_map = conf.get("impact", {})
            if metric_id not in conf_impact_map:
                continue

            magnitude = conf_impact_map[metric_id]
            start = _parse_date(conf["date"])
            duration = conf.get("duration_days", 1)
            end = start + timedelta(days=duration)
            # Confounders use a simpler envelope: instant onset, linear decay
            ramp_up = 0
            decay = conf.get("decay_days", max(1, duration // 2))

            affected_markets = conf.get("affected_markets", ["ALL"])
            affected_categories = conf.get("affected_categories", ["ALL"])

            if "ALL" in affected_markets:
                targeted_m_indices = list(range(len(market_ids)))
            else:
                targeted_m_indices = [
                    market_idx_map[m] for m in affected_markets if m in market_idx_map
                ]

            if "ALL" in affected_categories:
                targeted_c_indices = list(range(len(category_ids)))
            else:
                targeted_c_indices = [
                    cat_idx_map[c] for c in affected_categories if c in cat_idx_map
                ]

            for di, d in enumerate(dates):
                dt = d.to_pydatetime()
                envelope = _trapezoidal_envelope(dt, start, end, ramp_up, decay)
                if envelope <= 0.0:
                    continue

                impact_val = magnitude * envelope

                for mi in targeted_m_indices:
                    for ci in targeted_c_indices:
                        key = (di, mi, ci)
                        impacts[key] = impacts.get(key, 0.0) + impact_val

        return impacts

    # ------------------------------------------------------------------
    # Clamping
    # ------------------------------------------------------------------

    @staticmethod
    def _clamp_values(values: np.ndarray, metric_unit: str) -> np.ndarray:
        """Clamp values to valid ranges based on metric unit."""
        if metric_unit == "rate":
            return np.clip(values, 0.0, 1.0)
        elif metric_unit == "ratio":
            return np.clip(values, 0.0, None)  # ratios can exceed 1 but not be negative
        else:
            # count, usd, points, hours, minutes — must be non-negative
            return np.clip(values, 0.0, None)

    # ------------------------------------------------------------------
    # Journey aggregates
    # ------------------------------------------------------------------

    def _generate_journey_aggregates(self) -> pd.DataFrame:
        """Generate weekly journey archetype distributions per market.

        Produces a simplified distribution over archetypes like
        "discovery", "engage", "monetize", "loyalty", "churn" for each
        market-week combination.
        """
        # Weekly dates
        weeks = pd.date_range(self._start_date, self._end_date, freq="W-MON")
        market_ids = [m["id"] for m in self._markets]
        archetypes = self._journey_archetypes

        rows = []
        for week in weeks:
            for market in self._markets:
                # Base distribution varies by market tier
                tier = market["tier"]
                if tier == 1:
                    base_dist = np.array([0.15, 0.30, 0.25, 0.20, 0.10])
                elif tier == 2:
                    base_dist = np.array([0.20, 0.28, 0.18, 0.15, 0.19])
                else:
                    base_dist = np.array([0.25, 0.25, 0.12, 0.10, 0.28])

                # Add temporal variation
                week_dt = week.to_pydatetime()
                day_of_year = week_dt.timetuple().tm_yday
                seasonal_shift = 0.03 * np.sin(2 * np.pi * day_of_year / 365)
                # Shift toward monetize during holiday seasons
                base_dist[2] += seasonal_shift
                base_dist[4] -= seasonal_shift

                # Add noise
                noise = self._rng.normal(0, 0.015, size=len(archetypes))
                dist = base_dist + noise
                dist = np.clip(dist, 0.01, None)  # no zeros
                dist = dist / dist.sum()  # re-normalize

                for archetype, share in zip(archetypes, dist):
                    rows.append({
                        "week_start": week,
                        "market_id": market["id"],
                        "archetype": archetype,
                        "share": round(float(share), 4),
                    })

        df = pd.DataFrame(rows)
        df["market_id"] = df["market_id"].astype("category")
        df["archetype"] = df["archetype"].astype("category")
        return df

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def _export_data(self, daily_df: pd.DataFrame, journey_df: pd.DataFrame) -> None:
        """Write all generated artefacts to the output directory."""
        self._output_dir.mkdir(parents=True, exist_ok=True)

        # 1. daily_metrics.parquet — the main dataset
        parquet_path = self._output_dir / "daily_metrics.parquet"
        daily_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info("Wrote %s (%.1f MB)", parquet_path, parquet_path.stat().st_size / 1e6)

        # 2. initiative_calendar.csv
        init_df = pd.DataFrame(self._initiatives)
        # Flatten the impact dict to a JSON string column for CSV compatibility
        if "impact" in init_df.columns:
            init_df["impact"] = init_df["impact"].apply(json.dumps)
        if "target_markets" in init_df.columns:
            init_df["target_markets"] = init_df["target_markets"].apply(json.dumps)
        if "target_segments" in init_df.columns:
            init_df["target_segments"] = init_df["target_segments"].apply(json.dumps)
        if "target_categories" in init_df.columns:
            init_df["target_categories"] = init_df["target_categories"].apply(json.dumps)
        init_path = self._output_dir / "initiative_calendar.csv"
        init_df.to_csv(init_path, index=False)
        logger.info("Wrote %s", init_path)

        # 3. offer_catalog.csv — simplified offer definitions
        offer_catalog = self._generate_offer_catalog()
        offer_path = self._output_dir / "offer_catalog.csv"
        offer_catalog.to_csv(offer_path, index=False)
        logger.info("Wrote %s", offer_path)

        # 4. metric_definitions.json
        metric_defs = {
            m["id"]: {
                k: v for k, v in m.items()
                if k not in ("base_values",)  # keep base_values for reference
            }
            for m in self._metrics
        }
        defs_path = self._output_dir / "metric_definitions.json"
        with open(defs_path, "w") as f:
            json.dump(metric_defs, f, indent=2, default=str)
        logger.info("Wrote %s", defs_path)

        # 5. seasonal_patterns.json — pre-computed monthly seasonal baselines
        seasonal_patterns = self._compute_seasonal_baselines()
        seasonal_path = self._output_dir / "seasonal_patterns.json"
        with open(seasonal_path, "w") as f:
            json.dump(seasonal_patterns, f, indent=2)
        logger.info("Wrote %s", seasonal_path)

        # 6. confounder_log.csv
        conf_df = pd.DataFrame(self._confounders)
        if "impact" in conf_df.columns:
            conf_df["impact"] = conf_df["impact"].apply(json.dumps)
        if "affected_markets" in conf_df.columns:
            conf_df["affected_markets"] = conf_df["affected_markets"].apply(json.dumps)
        if "affected_categories" in conf_df.columns:
            conf_df["affected_categories"] = conf_df["affected_categories"].apply(json.dumps)
        conf_path = self._output_dir / "confounder_log.csv"
        conf_df.to_csv(conf_path, index=False)
        logger.info("Wrote %s", conf_path)

        # 7. change_points.csv — detected structural change points
        change_points = self._detect_change_points(daily_df)
        cp_path = self._output_dir / "change_points.csv"
        change_points.to_csv(cp_path, index=False)
        logger.info("Wrote %s (%d change points)", cp_path, len(change_points))

        # 8. metric_movements_golden.csv — golden dataset for eval
        logger.info("Detecting metric movements and building golden dataset...")
        golden_df = detect_movements(daily_df, threshold_pct=0.06)
        golden_path = self._output_dir / "metric_movements_golden.csv"
        golden_df.to_csv(golden_path, index=False)
        logger.info("Wrote %s (%d golden records)", golden_path, len(golden_df))

        # 9. journey_aggregates.parquet
        journey_path = self._output_dir / "journey_aggregates.parquet"
        journey_df.to_parquet(journey_path, index=False, engine="pyarrow")
        logger.info("Wrote %s", journey_path)

    # ------------------------------------------------------------------
    # Offer catalog generation
    # ------------------------------------------------------------------

    def _generate_offer_catalog(self) -> pd.DataFrame:
        """Generate a simplified offer catalog derived from initiatives."""
        offers = []
        offer_types = [
            "percentage_discount", "fixed_discount", "bonus_points",
            "free_trial", "cashback", "bundle",
        ]
        for i, init in enumerate(self._initiatives):
            offer_id = f"OFFER_{i+1:03d}"
            offers.append({
                "offer_id": offer_id,
                "initiative_id": init.get("id", f"INIT_{i+1:03d}"),
                "name": init.get("name", f"Offer {i+1}"),
                "type": init.get("type", self._rng.choice(offer_types)),
                "start_date": init.get("start_date", ""),
                "end_date": init.get("end_date", ""),
                "discount_pct": round(float(self._rng.uniform(0.05, 0.40)), 2),
                "min_purchase_usd": round(float(self._rng.choice([0, 0.99, 4.99, 9.99])), 2),
                "max_redemptions_per_user": int(self._rng.choice([1, 1, 1, 3, 5])),
                "target_markets": json.dumps(init.get("target_markets", ["ALL"])),
                "target_segments": json.dumps(init.get("target_segments", ["ALL"])),
                "target_categories": json.dumps(init.get("target_categories", ["ALL"])),
                "status": init.get("status", "COMPLETED"),
            })
        return pd.DataFrame(offers)

    # ------------------------------------------------------------------
    # Seasonal baselines (pre-computed for export)
    # ------------------------------------------------------------------

    def _compute_seasonal_baselines(self) -> dict:
        """Compute average seasonal multiplier per month for each market x metric."""
        baselines: dict[str, dict[str, dict[str, float]]] = {}

        for market in self._markets:
            market_id = market["id"]
            baselines[market_id] = {}
            for metric in self._metrics:
                metric_id = metric["id"]
                monthly_vals: dict[str, float] = {}
                for month in range(1, 13):
                    # Sample the 15th of each month as representative
                    sample_date = datetime(2025, month, 15)
                    # Use first category as representative
                    s = compute_seasonality(sample_date, market_id, self._categories[0]["id"], metric_id)
                    monthly_vals[str(month)] = round(s, 4)
                baselines[market_id][metric_id] = monthly_vals

        return baselines

    # ------------------------------------------------------------------
    # Change point detection (simple rolling-window approach)
    # ------------------------------------------------------------------

    def _detect_change_points(self, daily_df: pd.DataFrame) -> pd.DataFrame:
        """Detect structural change points using a WoW comparison approach.

        For each metric x market combination, we compute the 7-day rolling
        mean and flag dates where the WoW change exceeds a threshold.
        """
        change_points = []
        threshold = 0.10  # 10% WoW change

        # Aggregate across categories and segments to get market-level signal
        market_daily = (
            daily_df
            .groupby(["date", "market_id", "metric_name"], observed=True)["value"]
            .mean()
            .reset_index()
        )

        for (market_id, metric_name), group in market_daily.groupby(["market_id", "metric_name"], observed=True):
            group = group.sort_values("date").reset_index(drop=True)
            if len(group) < 14:
                continue

            # 7-day rolling mean
            group["rolling_7d"] = group["value"].rolling(7, min_periods=7).mean()
            group["rolling_7d_lag"] = group["rolling_7d"].shift(7)

            # WoW change
            mask = group["rolling_7d_lag"] > 0
            group.loc[mask, "wow_change"] = (
                (group.loc[mask, "rolling_7d"] - group.loc[mask, "rolling_7d_lag"])
                / group.loc[mask, "rolling_7d_lag"]
            )

            # Find points exceeding threshold
            significant = group[group["wow_change"].abs() > threshold]
            for _, row in significant.iterrows():
                change_points.append({
                    "date": row["date"],
                    "market_id": market_id,
                    "metric_name": metric_name,
                    "wow_change_pct": round(float(row["wow_change"]) * 100, 2),
                    "direction": "increase" if row["wow_change"] > 0 else "decrease",
                    "rolling_7d_value": round(float(row["rolling_7d"]), 4),
                })

        cp_df = pd.DataFrame(change_points)
        if len(cp_df) > 0:
            cp_df = cp_df.sort_values(["date", "market_id", "metric_name"]).reset_index(drop=True)
        return cp_df


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _parse_date(date_str: str | datetime) -> datetime:
    """Parse an ISO date string or return a datetime as-is."""
    if isinstance(date_str, datetime):
        return date_str
    return datetime.fromisoformat(date_str)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Generate synthetic Google Play Loyalty & Offers data",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Generate full dataset with all 10 segments (~25M rows). Default is sampled (~3M rows).",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=18,
        help="Number of months of history to generate (default: 18).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/synthetic",
        help="Output directory for generated files (default: data/synthetic).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    generator = DataGenerator(
        output_dir=args.output_dir,
        months=args.months,
        full=args.full,
        seed=args.seed,
    )
    generator.generate()
