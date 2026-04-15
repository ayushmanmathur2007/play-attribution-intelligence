"""Layer 4: journey archetype clustering.

Input:  data/layer1_sessions/sessions.parquet
Output:
    data/layer4_ai_ready/archetype_definitions.parquet (the k clusters)
    data/layer4_ai_ready/archetypes_per_week.parquet   (distribution by week x dim)

Algorithm: engineer a fixed-dim feature vector per session, KMeans cluster,
auto-label each cluster by its dominant event-type distribution.
"""

from __future__ import annotations

import time
from collections import Counter

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from .. import config

EVENT_FEATURES = [
    "session_start",
    "search",
    "browse_category",
    "app_view",
    "app_install",
    "app_launch",
    "purchase",
    "offer_impression",
    "offer_click",
    "offer_redeem",
    "rating_submit",
    "session_end",
]


def _session_features(row: pd.Series) -> list[float]:
    seq = row["events_seq"]
    if seq is None or (hasattr(seq, "__len__") and len(seq) == 0):
        seq = []
    counter = Counter(seq)
    total = max(sum(counter.values()), 1)
    feats = [counter.get(et, 0) / total for et in EVENT_FEATURES]
    feats.append(float(row.get("duration_s", 0)) / 600.0)      # normalize ~10min
    feats.append(float(row.get("event_count", 0)) / 20.0)      # normalize ~20 events
    feats.append(float(row.get("had_install", 0)))
    feats.append(float(row.get("had_purchase", 0)))
    feats.append(float(row.get("had_offer_redeem", 0)))
    return feats


def _auto_label(cluster_sessions: pd.DataFrame) -> str:
    """Label a cluster by its dominant event pattern."""
    all_events = []
    for seq in cluster_sessions["events_seq"].head(500):
        if seq is None:
            continue
        if hasattr(seq, "__len__") and len(seq) == 0:
            continue
        all_events.extend(list(seq))
    if not all_events:
        return "empty_session"

    counter = Counter(all_events)
    # Drop bookends
    for k in ["session_start", "session_end"]:
        counter.pop(k, None)
    top = [e for e, _ in counter.most_common(3)]
    if not top:
        return "minimal_session"

    # Heuristic overrides for semantic clarity
    if cluster_sessions["had_purchase"].mean() > 0.3:
        return f"buyer_{top[0]}"
    if cluster_sessions["had_offer_redeem"].mean() > 0.3:
        return f"offer_redeemer_{top[0] if top else 'generic'}"
    if cluster_sessions["had_install"].mean() > 0.3:
        return f"installer_{top[0]}"
    if "search" in top[:1]:
        return "searcher_" + "_".join(top[1:3]) if len(top) > 1 else "pure_searcher"
    return "browser_" + "_".join(top[:2])


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    t0 = time.time()
    sessions = pd.read_parquet(config.LAYER1_DIR / "sessions.parquet")
    sessions["date"] = pd.to_datetime(sessions["date"])

    # Feature engineering
    feat_rows = [_session_features(r) for _, r in sessions.iterrows()]
    X = np.array(feat_rows, dtype=float)

    # Cluster
    k = min(config.NUM_ARCHETYPES, max(2, len(sessions) // 50))
    kmeans = KMeans(n_clusters=k, random_state=config.SEED, n_init=10)
    sessions["archetype_id"] = kmeans.fit_predict(X)

    # Auto-label each cluster
    labels = {}
    for cid in range(k):
        cluster_sessions = sessions[sessions["archetype_id"] == cid]
        labels[cid] = _auto_label(cluster_sessions)

    definitions = pd.DataFrame(
        [
            {
                "archetype_id": cid,
                "archetype_name": labels[cid],
                "session_count": int((sessions["archetype_id"] == cid).sum()),
                "share_total": float((sessions["archetype_id"] == cid).mean()),
                "mean_duration_s": float(
                    sessions.loc[sessions["archetype_id"] == cid, "duration_s"].mean()
                ),
                "conversion_rate": float(
                    sessions.loc[sessions["archetype_id"] == cid, "had_purchase"].mean()
                ),
                "offer_redeem_rate": float(
                    sessions.loc[
                        sessions["archetype_id"] == cid, "had_offer_redeem"
                    ].mean()
                ),
            }
            for cid in range(k)
        ]
    )

    # Per-(week x dim_key) archetype distribution
    sessions["week_start"] = sessions["date"].dt.to_period("W-MON").dt.start_time
    sessions["dim_key"] = (
        sessions["market"].astype(str)
        + "/"
        + sessions["category"].astype(str)
        + "/"
        + sessions["segment"].astype(str)
    )

    per_week = (
        sessions.groupby(["week_start", "dim_key", "archetype_id"])
        .size()
        .reset_index(name="n_sessions")
    )
    totals = (
        sessions.groupby(["week_start", "dim_key"])
        .size()
        .reset_index(name="total_sessions")
    )
    per_week = per_week.merge(totals, on=["week_start", "dim_key"])
    per_week["session_share"] = per_week["n_sessions"] / per_week["total_sessions"]
    per_week["archetype_name"] = per_week["archetype_id"].map(labels)

    # WoW delta per (dim, archetype)
    per_week = per_week.sort_values(["dim_key", "archetype_id", "week_start"])
    per_week["wow_delta"] = per_week.groupby(["dim_key", "archetype_id"])[
        "session_share"
    ].diff().fillna(0.0)

    def_path = config.LAYER4_DIR / "archetype_definitions.parquet"
    wk_path = config.LAYER4_DIR / "archetypes_per_week.parquet"
    definitions.to_parquet(def_path, index=False, compression="snappy")
    per_week.to_parquet(wk_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer4/archetype] {k} clusters, "
        f"{len(per_week):,} (week,dim,archetype) rows "
        f"in {elapsed:.1f}s"
    )
    return definitions, per_week


if __name__ == "__main__":
    run()
