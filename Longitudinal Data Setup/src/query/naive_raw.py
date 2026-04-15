"""Naive path: answer longitudinal questions by scanning raw events.

This is the "before" in the before/after demo. Every question does a full
pass (or close to it) over `data/raw/events_*.parquet` — millions of rows
in the prototype, extrapolated to ~100B/day at Play scale.

Each query returns a `QueryResult` with wall time, rows scanned, and bytes
scanned, plus a prose answer. The UI puts this next to the `ai_ready`
result of the same question.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pandas as pd

from .. import config
from . import QueryResult

# Rough extrapolation: prototype simulates ~1k users x ~1 session/day x ~8
# events = ~8k events/day. Real Play ~= 100B events/day. Ratio ~1.25e7x.
# We expose this as a setting so the UI can show a "at real scale this
# would have been X rows" badge.
EXTRAPOLATION_FACTOR = 12_500_000


def _raw_stats() -> tuple[int, int]:
    """Rows + bytes currently sitting on disk in data/raw."""
    total_bytes = 0
    for p in Path(config.RAW_DIR).glob("events_*.parquet"):
        total_bytes += p.stat().st_size
    # Cheap row count via DuckDB
    con = duckdb.connect()
    raw_glob = str(config.RAW_DIR / "events_*.parquet")
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{raw_glob}')"
    ).fetchone()[0]
    return int(n), int(total_bytes)


def _scale(rows: int, bytes_: int) -> tuple[int, int]:
    return rows * EXTRAPOLATION_FACTOR, bytes_ * EXTRAPOLATION_FACTOR


# ---------------------------------------------------------------------------
# Q1: "What caused the conversion spike in IN/Games during Diwali 2024?"
# Naive answer: filter raw events, bucket by day, compare pre/post, guess.
# ---------------------------------------------------------------------------


def q_diwali_conversion_spike() -> QueryResult:
    question = "What caused the conversion-rate spike in IN/Games in late Oct 2024?"
    t0 = time.time()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    raw_glob = str(config.RAW_DIR / "events_*.parquet")

    sql = f"""
    WITH f AS (
        SELECT
            CAST(ts AS DATE) AS d,
            event_type,
            revenue_usd,
            session_id
        FROM read_parquet('{raw_glob}')
        WHERE market = 'IN'
          AND category = 'Games'
          AND ts >= DATE '2024-10-01'
          AND ts <  DATE '2024-11-15'
    )
    SELECT
        d,
        COUNT(DISTINCT session_id)                                   AS sessions,
        SUM(CAST(event_type = 'purchase'     AS INT))                AS purchases,
        SUM(CAST(event_type = 'offer_redeem' AS INT))                AS offer_redeems,
        SUM(revenue_usd)                                             AS revenue
    FROM f
    GROUP BY 1
    ORDER BY 1
    """
    df = con.execute(sql).fetch_df()
    wall = time.time() - t0

    # "Answer" — the naive path only has numbers, not a causal story.
    if df.empty:
        answer = "No rows matched. Cannot comment on the spike."
    else:
        df["conv"] = df["purchases"] / df["sessions"].clip(lower=1)
        peak_day = df.loc[df["conv"].idxmax()]
        answer = (
            f"Raw scan: conversion peaked on {peak_day['d']} at "
            f"{peak_day['conv'] * 100:.1f}% "
            f"({int(peak_day['purchases'])} purchases / {int(peak_day['sessions'])} sessions). "
            f"Cause: *unknown* — no context in raw events. You'd need to cross-reference "
            f"a marketing calendar the pipeline never joins against."
        )

    n_rows, n_bytes = _raw_stats()
    ext_rows, ext_bytes = _scale(n_rows, n_bytes)
    return QueryResult(
        path="naive_raw",
        question=question,
        answer=answer,
        facts=df.head(20).to_dict(orient="records"),
        wall_time_s=wall,
        rows_scanned_local=n_rows,
        bytes_scanned_local=n_bytes,
        rows_scanned_extrapolated=ext_rows,
        bytes_scanned_extrapolated=ext_bytes,
    )


# ---------------------------------------------------------------------------
# Q2: "Which weeks in the last 12 months had unusual (non-trend) movement
# in offer redemption rate globally?"
# Naive answer: compute weekly ratios, eyeball — no residual/trend split.
# ---------------------------------------------------------------------------


def q_offer_redemption_anomalies() -> QueryResult:
    question = "Which weeks saw unusual (non-trend) offer-redemption-rate movement?"
    t0 = time.time()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    raw_glob = str(config.RAW_DIR / "events_*.parquet")

    sql = f"""
    SELECT
        DATE_TRUNC('week', ts) AS wk,
        SUM(CAST(event_type = 'offer_redeem' AS INT)) AS redeems,
        SUM(CAST(event_type = 'offer_click'  AS INT)) AS clicks
    FROM read_parquet('{raw_glob}')
    GROUP BY 1
    ORDER BY 1
    """
    df = con.execute(sql).fetch_df()
    wall = time.time() - t0
    if df.empty:
        answer = "No rows."
    else:
        df["rate"] = df["redeems"] / df["clicks"].clip(lower=1)
        df["wow_delta"] = df["rate"].pct_change().fillna(0)
        big = df[df["wow_delta"].abs() > 0.10]
        answer = (
            f"Raw scan returned {len(big)} weeks with >10% WoW movement in "
            f"redemption rate — but *trend vs residual is not separated*, so "
            f"slow seasonal ramps show up next to real shocks. A human would "
            f"need to manually fit a trend here."
        )

    n_rows, n_bytes = _raw_stats()
    ext_rows, ext_bytes = _scale(n_rows, n_bytes)
    return QueryResult(
        path="naive_raw",
        question=question,
        answer=answer,
        facts=df.tail(20).to_dict(orient="records"),
        wall_time_s=wall,
        rows_scanned_local=n_rows,
        bytes_scanned_local=n_bytes,
        rows_scanned_extrapolated=ext_rows,
        bytes_scanned_extrapolated=ext_bytes,
    )


# ---------------------------------------------------------------------------
# Q3: "How has the user-behavior mix in US/Shopping changed over 12 months?"
# Naive answer: group by user + time → would need session reconstruction.
# We do a simplified "count distinct event types per user per month" pass.
# ---------------------------------------------------------------------------


def q_us_shopping_behavior_mix() -> QueryResult:
    question = "How has the user-behavior mix in US/Shopping changed over 12 months?"
    t0 = time.time()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    raw_glob = str(config.RAW_DIR / "events_*.parquet")

    sql = f"""
    SELECT
        DATE_TRUNC('month', ts) AS m,
        event_type,
        COUNT(*) AS n
    FROM read_parquet('{raw_glob}')
    WHERE market = 'US' AND category = 'Shopping'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df = con.execute(sql).fetch_df()
    wall = time.time() - t0

    answer = (
        f"Raw scan returned {len(df)} (month, event_type) rows for US/Shopping. "
        f"That gives an event-mix time series — but not an *archetype* mix. "
        f"To talk about 'deal hunters' or 'committed buyers' you'd need to "
        f"engineer per-session features, cluster, and track cluster shares "
        f"— work the naive path leaves entirely to the caller."
    )

    n_rows, n_bytes = _raw_stats()
    ext_rows, ext_bytes = _scale(n_rows, n_bytes)
    return QueryResult(
        path="naive_raw",
        question=question,
        answer=answer,
        facts=df.head(20).to_dict(orient="records"),
        wall_time_s=wall,
        rows_scanned_local=n_rows,
        bytes_scanned_local=n_bytes,
        rows_scanned_extrapolated=ext_rows,
        bytes_scanned_extrapolated=ext_bytes,
    )


# Map question-id → function, so the UI can iterate
QUESTIONS: dict[str, callable] = {
    "diwali_conversion_spike": q_diwali_conversion_spike,
    "offer_redemption_anomalies": q_offer_redemption_anomalies,
    "us_shopping_behavior_mix": q_us_shopping_behavior_mix,
}
