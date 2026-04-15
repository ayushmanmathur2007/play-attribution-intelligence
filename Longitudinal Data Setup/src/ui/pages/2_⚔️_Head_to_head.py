"""Naive vs AI-ready, same questions, cell-by-cell. The hero page.

Each question renders two columns — left: scan raw events.
right: look up layer-4. Latency, rows, bytes (local + extrapolated),
and the actual answer text all compared side-by-side.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.query import ai_ready, naive_raw  # noqa: E402

st.set_page_config(page_title="Head-to-head", page_icon="⚔️", layout="wide")

st.title("⚔️ Head-to-head: raw scan vs AI-ready")
st.caption(
    "Three longitudinal questions, two paths, same answer. "
    "Watch the latency and bytes-scanned columns — the numeric contrast is "
    "the whole point of Layer 4."
)

QUESTIONS = [
    (
        "diwali_conversion_spike",
        "Q1. What caused the conversion spike in IN/Games in late Oct 2024?",
        "The dumb version of this question scans all October India events and "
        "hopes you recognize the spike by eye. Layer 4 has the change-point "
        "pre-indexed AND the ranked list of plausible causes.",
    ),
    (
        "offer_redemption_anomalies",
        "Q2. Which weeks had unusual (non-trend) offer-redemption movement?",
        "Raw events don't know what 'trend' means. Layer 3 decomposes each "
        "weekly series into trend + seasonal + residual; anomalies are just "
        "a filter on residual_zscore.",
    ),
    (
        "us_shopping_behavior_mix",
        "Q3. How has US/Shopping user-behavior mix shifted over 12 months?",
        "Naive path: count event types by month — that's an event mix, not a "
        "user mix. Layer 4: KMeans clusters over session features + pre-tracked "
        "share of each cluster by week = archetype mix over time.",
    ),
]

selected = st.radio(
    "Question",
    [q[0] for q in QUESTIONS],
    format_func=lambda k: next(q[1] for q in QUESTIONS if q[0] == k),
    horizontal=False,
)

_, title, context = next(q for q in QUESTIONS if q[0] == selected)
st.markdown(f"### {title}")
st.caption(context)

run = st.button("▶︎ Run both paths", type="primary")

if run:
    with st.spinner("Running both paths..."):
        naive_result = naive_raw.QUESTIONS[selected]()
        ai_result = ai_ready.QUESTIONS[selected]()

    left, right = st.columns(2)

    def render(col, res, tint: str) -> None:
        with col:
            st.markdown(f"#### {tint} {res.path.replace('_', ' ')}")
            st.metric("Wall time", f"{res.wall_time_s * 1000:.0f} ms")
            c1, c2 = st.columns(2)
            c1.metric(
                "Rows scanned (local)",
                f"{res.rows_scanned_local:,}",
            )
            c2.metric(
                "Bytes scanned (local)",
                f"{res.bytes_scanned_local / 1024 / 1024:.2f} MB",
            )
            c3, c4 = st.columns(2)
            c3.metric(
                "→ extrapolated rows",
                f"{res.rows_scanned_extrapolated / 1e9:.2f} B"
                if res.rows_scanned_extrapolated > 1e8
                else f"{res.rows_scanned_extrapolated:,}",
            )
            c4.metric(
                "→ extrapolated bytes",
                f"{res.bytes_scanned_extrapolated / 1e12:.2f} TB"
                if res.bytes_scanned_extrapolated > 1e11
                else f"{res.bytes_scanned_extrapolated / 1e9:.2f} GB",
            )
            st.markdown("**Answer:**")
            st.markdown(res.answer)
            if res.extras.get("files_touched"):
                with st.expander("Files consulted"):
                    for f in res.extras["files_touched"]:
                        st.code(f)
            if res.facts:
                with st.expander(f"Raw evidence ({len(res.facts)} rows)"):
                    st.dataframe(pd.DataFrame(res.facts), use_container_width=True, hide_index=True)

    render(left, naive_result, "🐢")
    render(right, ai_result, "🚀")

    # Summary delta
    speedup = (
        naive_result.wall_time_s / ai_result.wall_time_s
        if ai_result.wall_time_s > 0
        else float("inf")
    )
    byte_ratio = (
        naive_result.bytes_scanned_extrapolated
        / max(ai_result.bytes_scanned_extrapolated, 1)
    )
    st.divider()
    st.success(
        f"**Speedup:** {speedup:.1f}x wall time · "
        f"**Data touched:** {byte_ratio:,.0f}x less at real scale · "
        f"**Story:** the AI path returns a causal hypothesis, not just numbers."
    )
else:
    st.info("Click **Run both paths** to execute the comparison.")
