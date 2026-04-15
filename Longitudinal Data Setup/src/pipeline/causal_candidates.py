"""Layer 4: causal candidate index — the genuinely novel artifact.

For every detected change point, pre-compute a ranked list of plausible
causes drawn from:
  (a) initiatives active in a window around the change date
  (b) other change points in the same window on related dimensions
  (c) archetype composition shifts in the same week

This is the analyst's "first hypothesis list" — not proof, but an
agent-ready shortcut. Humans spend hours on step one of causal
investigations; pre-computing it flips the work from "what should I
check?" to "which of these 3 candidates is right?"

Input:
    data/layer4_ai_ready/change_points.parquet
    data/layer4_ai_ready/archetypes_per_week.parquet (optional)
    config.INITIATIVES (in-memory — initiative calendar is our ground truth)
Output:
    data/layer4_ai_ready/causal_candidates.parquet

Schema (long format — one row per (change_id, candidate)):
    change_id, metric, dim_key, change_date,
    candidate_type ('initiative' | 'co_change' | 'archetype_shift'),
    candidate_id,
    candidate_name,
    score,                  float in [0, 1]
    rationale               short human-readable string
"""

from __future__ import annotations

import time
from datetime import timedelta

import pandas as pd

from .. import config

# Window for "contemporaneous" effects — change points within ±2 weeks of an
# initiative start date are considered candidates.
WINDOW_DAYS = 14


# ---------------------------------------------------------------------------
# Candidate: initiative overlap
# ---------------------------------------------------------------------------


def _initiative_candidates(row: pd.Series) -> list[dict]:
    """For one change point, find all initiatives that overlap its window."""
    cp_date = pd.Timestamp(row["change_date"]).to_pydatetime().date()
    cp_market, cp_category, _ = row["dim_key"].split("/")
    metric = row["metric"]
    out: list[dict] = []

    for init in config.INITIATIVES:
        # Dimension match — None means "global" and always matches
        if init["market"] is not None and init["market"] != cp_market:
            continue
        if init["category"] is not None and init["category"] != cp_category:
            continue

        start = init["start"]
        end = init["end"]
        window_start = start - timedelta(days=WINDOW_DAYS)
        window_end = end + timedelta(days=WINDOW_DAYS)
        if not (window_start <= cp_date <= window_end):
            continue

        # Score components:
        #   1. Temporal closeness — how close is the CP to the initiative start?
        days_from_start = abs((cp_date - start).days)
        temporal_score = max(0.0, 1.0 - days_from_start / (2 * WINDOW_DAYS))

        #   2. Metric alignment — does the initiative claim to affect this metric?
        effects = init["effects"]
        metric_match = 0.0
        for k in effects.keys():
            if k in metric or metric in k:
                metric_match = 1.0
                break
        if metric_match == 0.0:
            # Partial match: initiatives that move offer_redeem / purchase are
            # likely to also move conversion_rate, revenue_per_session, etc.
            ratio_metrics = {
                "conversion_rate": ["purchase"],
                "install_rate": ["app_install"],
                "offer_ctr": ["offer_click", "offer_impression"],
                "offer_redemption_rate": ["offer_redeem", "offer_click"],
                "revenue_per_session": ["purchase"],
            }
            related = ratio_metrics.get(metric, [])
            if any(r in effects for r in related):
                metric_match = 0.6

        #   3. Direction alignment — does the initiative push the same way?
        #      Initiative effects > 1 push up; the CP has a direction field.
        effect_values = [v for k, v in effects.items() if isinstance(v, (int, float))]
        if effect_values:
            init_direction = "up" if max(effect_values) > 1 else "down"
            direction_match = 1.0 if init_direction == row["direction"] else 0.2
        else:
            direction_match = 0.5

        score = 0.5 * temporal_score + 0.3 * metric_match + 0.2 * direction_match

        rationale = (
            f"{init['name']} ran {start}..{end} "
            f"({'matching' if metric_match > 0.9 else 'adjacent'} metric, "
            f"{days_from_start}d from start)"
        )

        out.append(
            {
                "candidate_type": "initiative",
                "candidate_id": init["id"],
                "candidate_name": init["name"],
                "score": float(score),
                "rationale": rationale,
            }
        )

    return out


# ---------------------------------------------------------------------------
# Candidate: co-occurring change points on related dims
# ---------------------------------------------------------------------------


def _cochange_candidates(
    row: pd.Series, all_changes: pd.DataFrame
) -> list[dict]:
    """Other change points in the same (market, window) might share a cause."""
    if all_changes.empty:
        return []

    cp_date = pd.Timestamp(row["change_date"])
    cp_market = row["dim_key"].split("/")[0]
    window_start = cp_date - pd.Timedelta(days=WINDOW_DAYS)
    window_end = cp_date + pd.Timedelta(days=WINDOW_DAYS)

    neighbors = all_changes[
        (all_changes["change_id"] != row["change_id"])
        & (all_changes["change_date"] >= window_start)
        & (all_changes["change_date"] <= window_end)
        & (all_changes["dim_key"].str.startswith(cp_market + "/"))
    ]

    out: list[dict] = []
    for _, n in neighbors.head(5).iterrows():
        day_diff = abs((pd.Timestamp(n["change_date"]) - cp_date).days)
        temporal_score = max(0.0, 1.0 - day_diff / WINDOW_DAYS)
        direction_bonus = 0.2 if n["direction"] == row["direction"] else 0.0
        score = 0.5 * temporal_score + direction_bonus

        rationale = (
            f"{n['metric']} on {n['dim_key']} also {n['direction']} "
            f"{n['magnitude_pct'] * 100:+.1f}% "
            f"({day_diff}d away) — may share a driver"
        )

        out.append(
            {
                "candidate_type": "co_change",
                "candidate_id": n["change_id"],
                "candidate_name": f"{n['metric']} / {n['dim_key']}",
                "score": float(score),
                "rationale": rationale,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Candidate: archetype composition shift
# ---------------------------------------------------------------------------


def _archetype_candidates(
    row: pd.Series, archetypes: pd.DataFrame
) -> list[dict]:
    """Was there a big shift in archetype mix the same week?"""
    if archetypes.empty:
        return []

    cp_week = pd.Timestamp(row["change_date"]).to_period("W-MON").start_time
    same_week = archetypes[
        (archetypes["week_start"] == cp_week) & (archetypes["dim_key"] == row["dim_key"])
    ]
    if same_week.empty:
        return []

    # Look at biggest |wow_delta|
    movers = same_week.reindex(
        same_week["wow_delta"].abs().sort_values(ascending=False).index
    ).head(3)
    out: list[dict] = []
    for _, m in movers.iterrows():
        if abs(m["wow_delta"]) < 0.03:
            continue
        # Simple score: proportional to shift magnitude, capped
        score = float(min(1.0, abs(m["wow_delta"]) * 5))
        direction_label = "grew" if m["wow_delta"] > 0 else "shrank"
        rationale = (
            f"'{m['archetype_name']}' archetype "
            f"{direction_label} {m['wow_delta'] * 100:+.1f}pp this week"
        )
        out.append(
            {
                "candidate_type": "archetype_shift",
                "candidate_id": f"arch_{m['archetype_id']}",
                "candidate_name": str(m["archetype_name"]),
                "score": score,
                "rationale": rationale,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> pd.DataFrame:
    t0 = time.time()

    cp_path = config.LAYER4_DIR / "change_points.parquet"
    if not cp_path.exists():
        print("  [layer4/causal] no change_points.parquet — skipping")
        empty = pd.DataFrame(
            columns=[
                "change_id",
                "metric",
                "dim_key",
                "change_date",
                "candidate_type",
                "candidate_id",
                "candidate_name",
                "score",
                "rationale",
            ]
        )
        empty.to_parquet(
            config.LAYER4_DIR / "causal_candidates.parquet",
            index=False,
            compression="snappy",
        )
        return empty

    change_points = pd.read_parquet(cp_path)
    if change_points.empty:
        print("  [layer4/causal] change_points table is empty")
        empty = pd.DataFrame(
            columns=[
                "change_id",
                "metric",
                "dim_key",
                "change_date",
                "candidate_type",
                "candidate_id",
                "candidate_name",
                "score",
                "rationale",
            ]
        )
        empty.to_parquet(
            config.LAYER4_DIR / "causal_candidates.parquet",
            index=False,
            compression="snappy",
        )
        return empty

    change_points["change_date"] = pd.to_datetime(change_points["change_date"])

    arch_path = config.LAYER4_DIR / "archetypes_per_week.parquet"
    if arch_path.exists():
        archetypes = pd.read_parquet(arch_path)
        archetypes["week_start"] = pd.to_datetime(archetypes["week_start"])
    else:
        archetypes = pd.DataFrame()

    rows: list[dict] = []
    for _, cp in change_points.iterrows():
        base = {
            "change_id": cp["change_id"],
            "metric": cp["metric"],
            "dim_key": cp["dim_key"],
            "change_date": cp["change_date"],
        }
        for cand in _initiative_candidates(cp):
            rows.append({**base, **cand})
        for cand in _cochange_candidates(cp, change_points):
            rows.append({**base, **cand})
        for cand in _archetype_candidates(cp, archetypes):
            rows.append({**base, **cand})

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(
            columns=[
                "change_id",
                "metric",
                "dim_key",
                "change_date",
                "candidate_type",
                "candidate_id",
                "candidate_name",
                "score",
                "rationale",
            ]
        )
    else:
        df = df.sort_values(["change_id", "score"], ascending=[True, False])

    out_path = config.LAYER4_DIR / "causal_candidates.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    n_cps = change_points["change_id"].nunique()
    print(
        f"  [layer4/causal] {len(df):,} candidates across {n_cps} change points "
        f"-> {out_path.name} in {elapsed:.1f}s"
    )
    return df


if __name__ == "__main__":
    run()
