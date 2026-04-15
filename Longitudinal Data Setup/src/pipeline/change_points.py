"""Layer 4: materialized change-point detection.

Input:  data/layer3_decomposed/weekly_decomposed.parquet
Output: data/layer4_ai_ready/change_points.parquet

Primary detector: rolling Z-score on the residual column (fast, no deps).
Bonus detector: ruptures.Pelt if the `ruptures` library is available.
Both results are persisted with a detection_method column so the demo
can show them side-by-side.
"""

from __future__ import annotations

import time
import uuid

import numpy as np
import pandas as pd

from .. import config


def _rolling_zscore_changepoints(
    group: pd.DataFrame, z_threshold: float, min_pct: float
) -> list[dict]:
    """Detect change points by thresholding |residual z-score| on the Layer 3 series."""
    out = []
    group = group.sort_values("week_start").reset_index(drop=True)
    if len(group) < 4:
        return out

    # Rolling mean over 4-week windows before and after each point
    for i in range(3, len(group) - 3):
        z = group.loc[i, "residual_zscore"]
        if not np.isfinite(z) or abs(z) < z_threshold:
            continue

        pre = group.loc[i - 3 : i - 1, "raw"].mean()
        post = group.loc[i + 1 : i + 3, "raw"].mean()
        if pre == 0 or not np.isfinite(pre) or not np.isfinite(post):
            continue
        magnitude_pct = (post - pre) / abs(pre)
        if abs(magnitude_pct) < min_pct:
            continue

        out.append(
            {
                "change_id": str(uuid.uuid4())[:8],
                "metric": group.loc[i, "metric"],
                "dim_key": group.loc[i, "dim_key"],
                "change_date": group.loc[i, "week_start"],
                "direction": "up" if magnitude_pct > 0 else "down",
                "magnitude_pct": float(magnitude_pct),
                "confidence": float(min(1.0, abs(z) / 5.0)),
                "pre_mean": float(pre),
                "post_mean": float(post),
                "detection_method": "rolling_zscore",
            }
        )
    return out


def _ruptures_changepoints(group: pd.DataFrame, min_pct: float) -> list[dict]:
    """Detect change points using ruptures.Pelt (if installed)."""
    try:
        import ruptures as rpt
    except ImportError:
        return []

    group = group.sort_values("week_start").reset_index(drop=True)
    if len(group) < 12:
        return []

    signal = group["raw"].values.reshape(-1, 1)
    try:
        algo = rpt.Pelt(model="rbf").fit(signal)
        bkps = algo.predict(pen=3.0)
    except Exception:
        return []

    out = []
    for b in bkps[:-1]:  # last breakpoint is end-of-series
        i = int(b)
        if i < 3 or i >= len(group) - 3:
            continue
        pre = group.loc[i - 3 : i - 1, "raw"].mean()
        post = group.loc[i + 1 : min(i + 3, len(group) - 1), "raw"].mean()
        if pre == 0 or not np.isfinite(pre) or not np.isfinite(post):
            continue
        magnitude_pct = (post - pre) / abs(pre)
        if abs(magnitude_pct) < min_pct:
            continue
        out.append(
            {
                "change_id": str(uuid.uuid4())[:8],
                "metric": group.loc[i, "metric"],
                "dim_key": group.loc[i, "dim_key"],
                "change_date": group.loc[i, "week_start"],
                "direction": "up" if magnitude_pct > 0 else "down",
                "magnitude_pct": float(magnitude_pct),
                "confidence": 0.80,  # Pelt doesn't give a natural probability
                "pre_mean": float(pre),
                "post_mean": float(post),
                "detection_method": "ruptures_pelt",
            }
        )
    return out


def run() -> pd.DataFrame:
    t0 = time.time()
    weekly = pd.read_parquet(config.LAYER3_DIR / "weekly_decomposed.parquet")
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])

    all_changes: list[dict] = []
    for (metric, dim_key), group in weekly.groupby(["metric", "dim_key"]):
        all_changes.extend(
            _rolling_zscore_changepoints(
                group,
                z_threshold=config.CHANGE_POINT_Z_THRESHOLD,
                min_pct=config.CHANGE_POINT_MIN_MAGNITUDE_PCT,
            )
        )
        all_changes.extend(
            _ruptures_changepoints(group, min_pct=config.CHANGE_POINT_MIN_MAGNITUDE_PCT)
        )

    df = pd.DataFrame(all_changes)
    if df.empty:
        # Emit an empty frame with the expected schema so downstream code doesn't explode
        df = pd.DataFrame(
            columns=[
                "change_id",
                "metric",
                "dim_key",
                "change_date",
                "direction",
                "magnitude_pct",
                "confidence",
                "pre_mean",
                "post_mean",
                "detection_method",
            ]
        )

    out_path = config.LAYER4_DIR / "change_points.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer4/cp] {len(df):,} change points detected "
        f"-> {out_path.name} in {elapsed:.1f}s"
    )
    return df


if __name__ == "__main__":
    run()
