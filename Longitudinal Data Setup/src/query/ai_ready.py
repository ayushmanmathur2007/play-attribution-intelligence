"""AI-ready path: answer longitudinal questions by reading Layer-4 artifacts.

Each query loads ONE parquet per question (sometimes two) — never raw events.
The agent's "evidence trail" is a list of filenames + row counts so a human
reviewer can see exactly what was consulted.

Contrast with `naive_raw`: same questions, same answers, but now the story
and the candidate causes are pre-materialized.
"""

from __future__ import annotations

import time

import pandas as pd

from .. import config
from . import QueryResult


def _ai_stats(files: list[str], rows: int) -> tuple[int, int]:
    """Approximate bytes read — sum of file sizes touched."""
    total_bytes = 0
    for f in files:
        p = config.LAYER4_DIR / f
        if p.exists():
            total_bytes += p.stat().st_size
        else:
            p3 = config.LAYER3_DIR / f
            if p3.exists():
                total_bytes += p3.stat().st_size
    return rows, total_bytes


# ---------------------------------------------------------------------------
# Q1: Diwali conversion spike — the hero example.
# ---------------------------------------------------------------------------


def q_diwali_conversion_spike() -> QueryResult:
    question = "What caused the conversion-rate spike in IN/Games in late Oct 2024?"
    t0 = time.time()

    # Step 1: locate the change point via the materialized index
    cps = pd.read_parquet(config.LAYER4_DIR / "change_points.parquet")
    if not cps.empty:
        cps["change_date"] = pd.to_datetime(cps["change_date"])
        target = cps[
            (cps["dim_key"] == "IN/Games/casual")
            | (cps["dim_key"].str.startswith("IN/Games/"))
        ]
        target = target[
            (target["change_date"] >= pd.Timestamp("2024-10-01"))
            & (target["change_date"] <= pd.Timestamp("2024-11-15"))
            & (target["metric"].isin(["conversion_rate", "offer_redemption_rate", "revenue_per_session"]))
        ]
    else:
        target = pd.DataFrame()

    # Step 2: look up causal candidates for any found change points
    candidates = pd.DataFrame()
    if not target.empty:
        causal = pd.read_parquet(config.LAYER4_DIR / "causal_candidates.parquet")
        candidates = causal[causal["change_id"].isin(target["change_id"])]

    # Step 3: narrative sentence for the same week
    narr_row = None
    try:
        narr = pd.read_parquet(config.LAYER4_DIR / "narrative_log.parquet")
        narr["week_start"] = pd.to_datetime(narr["week_start"])
        match = narr[
            (narr["dim_key"].str.startswith("IN/Games/"))
            & (narr["week_start"] >= pd.Timestamp("2024-10-20"))
            & (narr["week_start"] <= pd.Timestamp("2024-11-05"))
        ]
        if not match.empty:
            narr_row = match.iloc[0]
    except FileNotFoundError:
        pass

    wall = time.time() - t0

    # Synthesize answer from pre-computed facts
    if target.empty:
        answer = "No change point found in IN/Games for the requested window."
    else:
        cp = target.iloc[0]
        top_candidate = candidates.head(1).iloc[0] if not candidates.empty else None
        narrative_text = narr_row["body"] if narr_row is not None else ""
        answer_lines = [
            f"✅ Change point detected: {cp['metric']} {cp['direction']} "
            f"{cp['magnitude_pct'] * 100:+.1f}% in IN/Games on "
            f"{cp['change_date'].date()} "
            f"(confidence {cp['confidence']:.0%}).",
        ]
        if top_candidate is not None:
            answer_lines.append(
                f"Top candidate cause: **{top_candidate['candidate_name']}** "
                f"({top_candidate['candidate_type']}, score {top_candidate['score']:.2f}). "
                f"Rationale: {top_candidate['rationale']}"
            )
        if narrative_text:
            answer_lines.append(f"Narrative log: {narrative_text}")
        answer = "\n".join(answer_lines)

    files_touched = [
        "change_points.parquet",
        "causal_candidates.parquet",
        "narrative_log.parquet",
    ]
    rows_read = int(len(target) + len(candidates) + (1 if narr_row is not None else 0))
    rows, bytes_ = _ai_stats(files_touched, rows_read)
    facts = target.to_dict(orient="records") + candidates.head(5).to_dict(orient="records")
    return QueryResult(
        path="ai_ready",
        question=question,
        answer=answer,
        facts=facts,
        wall_time_s=wall,
        rows_scanned_local=rows,
        bytes_scanned_local=bytes_,
        rows_scanned_extrapolated=rows,  # doesn't scale with event volume
        bytes_scanned_extrapolated=bytes_,
        extras={"files_touched": files_touched},
    )


# ---------------------------------------------------------------------------
# Q2: offer redemption anomalies — trend-adjusted out of the box.
# ---------------------------------------------------------------------------


def q_offer_redemption_anomalies() -> QueryResult:
    question = "Which weeks saw unusual (non-trend) offer-redemption-rate movement?"
    t0 = time.time()
    weekly = pd.read_parquet(config.LAYER3_DIR / "weekly_decomposed.parquet")
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])

    mask = (weekly["metric"] == "offer_redemption_rate") & (weekly["residual_zscore"].abs() > 2.0)
    anomalies = weekly[mask].copy()
    anomalies = anomalies.sort_values("residual_zscore", key=lambda s: s.abs(), ascending=False)
    wall = time.time() - t0

    if anomalies.empty:
        answer = "No offer-redemption-rate weeks exceeded |z| > 2 on residuals."
    else:
        top = anomalies.head(5)
        bullets = [
            f"• {r['week_start'].date()} {r['dim_key']}: "
            f"raw={r['raw']:.3f}, residual z={r['residual_zscore']:.1f} "
            f"(WoW {r['wow_delta'] * 100:+.1f}%)"
            for _, r in top.iterrows()
        ]
        answer = (
            f"{len(anomalies)} weeks flagged as residual anomalies "
            f"(trend + seasonal already removed). Top 5:\n" + "\n".join(bullets)
        )

    files_touched = ["weekly_decomposed.parquet"]
    rows, bytes_ = _ai_stats(files_touched, len(anomalies))
    return QueryResult(
        path="ai_ready",
        question=question,
        answer=answer,
        facts=anomalies.head(20).to_dict(orient="records"),
        wall_time_s=wall,
        rows_scanned_local=rows,
        bytes_scanned_local=bytes_,
        rows_scanned_extrapolated=rows,
        bytes_scanned_extrapolated=bytes_,
        extras={"files_touched": files_touched},
    )


# ---------------------------------------------------------------------------
# Q3: US/Shopping behavior mix — archetypes per week, not raw events.
# ---------------------------------------------------------------------------


def q_us_shopping_behavior_mix() -> QueryResult:
    question = "How has the user-behavior mix in US/Shopping changed over 12 months?"
    t0 = time.time()
    arch = pd.read_parquet(config.LAYER4_DIR / "archetypes_per_week.parquet")
    arch["week_start"] = pd.to_datetime(arch["week_start"])
    mask = arch["dim_key"].str.startswith("US/Shopping/")
    view = arch[mask].copy()
    wall = time.time() - t0

    if view.empty:
        answer = "No archetype rows for US/Shopping."
    else:
        # Which archetype grew most over the full period?
        first = view[view["week_start"] == view["week_start"].min()]
        last = view[view["week_start"] == view["week_start"].max()]
        first_shares = first.groupby("archetype_name")["session_share"].sum()
        last_shares = last.groupby("archetype_name")["session_share"].sum()
        diff = (last_shares - first_shares).dropna().sort_values(ascending=False)
        top_grower = diff.head(1)
        top_shrinker = diff.tail(1)

        lines = []
        if not top_grower.empty:
            name, val = top_grower.index[0], top_grower.iloc[0]
            lines.append(f"Biggest grower: {name} {val * 100:+.1f}pp")
        if not top_shrinker.empty:
            name, val = top_shrinker.index[0], top_shrinker.iloc[0]
            lines.append(f"Biggest shrinker: {name} {val * 100:+.1f}pp")
        answer = (
            f"{view['archetype_name'].nunique()} archetypes tracked across "
            f"{view['week_start'].nunique()} weeks in US/Shopping.\n"
            + "\n".join(lines)
        )

    files_touched = ["archetypes_per_week.parquet"]
    rows, bytes_ = _ai_stats(files_touched, len(view))
    return QueryResult(
        path="ai_ready",
        question=question,
        answer=answer,
        facts=view.head(20).to_dict(orient="records"),
        wall_time_s=wall,
        rows_scanned_local=rows,
        bytes_scanned_local=bytes_,
        rows_scanned_extrapolated=rows,
        bytes_scanned_extrapolated=bytes_,
        extras={"files_touched": files_touched},
    )


QUESTIONS: dict[str, callable] = {
    "diwali_conversion_spike": q_diwali_conversion_spike,
    "offer_redemption_anomalies": q_offer_redemption_anomalies,
    "us_shopping_behavior_mix": q_us_shopping_behavior_mix,
}
