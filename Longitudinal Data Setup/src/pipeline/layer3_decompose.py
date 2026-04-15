"""Layer 3: weekly rollups with STL trend/seasonal/residual decomposition.

Input:  data/layer2_daily/daily.parquet
Output: data/layer3_decomposed/weekly_decomposed.parquet

Long-format output — one row per (metric, dim_key, week_start):
    metric, dim_key, week_start, raw, trend, seasonal, residual,
    wow_delta, yoy_delta, trend_slope_4w, residual_zscore

The critical design decision: decomposition happens here, not at query
time. Agents downstream read the pre-split columns and reason over
trend vs residual without ever running STL themselves.
"""

from __future__ import annotations

import time
from typing import Optional

import numpy as np
import pandas as pd

from .. import config

# Metrics we care about for longitudinal reasoning (ratios, not counts)
DECOMPOSABLE_METRICS = [
    "conversion_rate",
    "install_rate",
    "offer_ctr",
    "offer_redemption_rate",
    "revenue_per_session",
    "avg_session_duration_s",
    "avg_events_per_session",
]


def _try_stl(series: pd.Series, period: int) -> Optional[tuple]:
    """Run STL decomposition if statsmodels is available and series is long enough."""
    if len(series) < 2 * period:
        return None
    try:
        from statsmodels.tsa.seasonal import STL
    except ImportError:
        return None
    try:
        stl = STL(series, period=period, robust=True).fit()
        return stl.trend.values, stl.seasonal.values, stl.resid.values
    except Exception:
        return None


def _moving_avg_decompose(series: pd.Series, window: int = 8) -> tuple:
    """Fallback decomposition: centered moving average trend + residual."""
    trend = series.rolling(window=window, center=True, min_periods=1).mean()
    residual = series - trend
    seasonal = pd.Series(np.zeros(len(series)), index=series.index)
    return trend.values, seasonal.values, residual.values


def _decompose_series(series: pd.Series, period: int) -> pd.DataFrame:
    """Run STL (or fallback) on one metric series and return a long-format DataFrame."""
    stl_result = _try_stl(series, period)
    if stl_result is not None:
        trend, seasonal, residual = stl_result
    else:
        trend, seasonal, residual = _moving_avg_decompose(series)

    out = pd.DataFrame(
        {
            "week_start": series.index,
            "raw": series.values,
            "trend": trend,
            "seasonal": seasonal,
            "residual": residual,
        }
    )
    # WoW delta
    out["wow_delta"] = out["raw"].pct_change().fillna(0.0)
    # YoY delta (52-week lag)
    if len(out) >= 52:
        out["yoy_delta"] = out["raw"].pct_change(52).fillna(0.0)
    else:
        out["yoy_delta"] = 0.0
    # 4-week trend slope (linear regression over the trend component)
    slopes = []
    for i in range(len(out)):
        lo = max(0, i - 3)
        window = out["trend"].iloc[lo : i + 1].values
        if len(window) >= 2 and not np.isnan(window).any():
            x = np.arange(len(window))
            slope = np.polyfit(x, window, 1)[0]
        else:
            slope = 0.0
        slopes.append(slope)
    out["trend_slope_4w"] = slopes
    # Residual z-score
    resid_std = out["residual"].std() or 1e-9
    out["residual_zscore"] = out["residual"] / resid_std
    return out


def run() -> pd.DataFrame:
    t0 = time.time()
    daily = pd.read_parquet(config.LAYER2_DIR / "daily.parquet")
    daily["date"] = pd.to_datetime(daily["date"])

    # Build the weekly long-format table
    pieces = []
    for (market, category, segment), group in daily.groupby(
        ["market", "category", "segment"]
    ):
        dim_key = f"{market}/{category}/{segment}"
        group = group.sort_values("date").set_index("date")
        # Resample to weekly (W-MON = week starts Monday)
        weekly = group.resample("W-MON").mean(numeric_only=True)
        if len(weekly) < 4:
            continue

        for metric in DECOMPOSABLE_METRICS:
            if metric not in weekly.columns:
                continue
            series = weekly[metric].dropna()
            if len(series) < 4:
                continue
            decomp = _decompose_series(series, period=config.STL_WEEKLY_PERIOD)
            decomp["metric"] = metric
            decomp["dim_key"] = dim_key
            pieces.append(decomp)

    if not pieces:
        raise RuntimeError("Layer 3 produced zero rows — check Layer 2 output")

    out_df = pd.concat(pieces, ignore_index=True)
    out_df = out_df[
        [
            "metric",
            "dim_key",
            "week_start",
            "raw",
            "trend",
            "seasonal",
            "residual",
            "wow_delta",
            "yoy_delta",
            "trend_slope_4w",
            "residual_zscore",
        ]
    ]

    out_path = config.LAYER3_DIR / "weekly_decomposed.parquet"
    out_df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer3] {len(out_df):,} weekly rows "
        f"across {out_df['metric'].nunique()} metrics "
        f"x {out_df['dim_key'].nunique()} dims "
        f"-> {out_path.name} "
        f"({out_path.stat().st_size / 1024 / 1024:.1f} MB) "
        f"in {elapsed:.1f}s"
    )
    return out_df


if __name__ == "__main__":
    run()
