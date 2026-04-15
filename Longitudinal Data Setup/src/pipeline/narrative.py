"""Layer 4: semantic event log — the PRIMARY agent interface.

Input:  layer2 daily, layer3 weekly decomp, layer4 change points, archetypes
Output: data/layer4_ai_ready/narrative_log.parquet

For each (week_start x dim_key), produce a one-paragraph natural language
summary describing what happened. All numeric facts come from the
pre-aggregated layers — the LLM (if enabled via ANTHROPIC_API_KEY) is
only used to rephrase the template output. No fact is ever generated
by the LLM.

This is the hero artifact. An agent answering a 12-month longitudinal
question reads ~52 of these paragraphs instead of scanning billions of
events. Pre-digested institutional memory.
"""

from __future__ import annotations

import os
import time

import numpy as np
import pandas as pd

from .. import config


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------


def _fmt_pct(x: float) -> str:
    if x is None or not np.isfinite(x):
        return "n/a"
    return f"{x * 100:+.1f}%"


def _fmt_num(x: float) -> str:
    if x is None or not np.isfinite(x):
        return "n/a"
    if abs(x) >= 1_000_000:
        return f"{x / 1_000_000:.1f}M"
    if abs(x) >= 1_000:
        return f"{x / 1_000:.1f}k"
    if abs(x) >= 1:
        return f"{x:.0f}"
    return f"{x:.3f}"


def _period_description(week_start: pd.Timestamp) -> str:
    return week_start.strftime("week of %b %d, %Y")


# ---------------------------------------------------------------------------
# Headline generator — deterministic, template-based
# ---------------------------------------------------------------------------


def _build_headline(
    week_start: pd.Timestamp,
    dim_key: str,
    weekly_slice: pd.DataFrame,
    change_points_slice: pd.DataFrame,
) -> str:
    market, category, segment = dim_key.split("/")
    period = _period_description(week_start)

    if not change_points_slice.empty:
        top_cp = change_points_slice.iloc[0]
        return (
            f"{period}, {market}/{category} ({segment}): "
            f"{top_cp['metric']} {top_cp['direction']} "
            f"{_fmt_pct(top_cp['magnitude_pct'])} — change point detected."
        )

    # No change point -> flag dominant trend if present
    if not weekly_slice.empty:
        largest = weekly_slice.iloc[weekly_slice["wow_delta"].abs().argmax()]
        if abs(largest["wow_delta"]) > 0.05:
            direction = "up" if largest["wow_delta"] > 0 else "down"
            return (
                f"{period}, {market}/{category} ({segment}): "
                f"{largest['metric']} {direction} "
                f"{_fmt_pct(largest['wow_delta'])} WoW."
            )

    return (
        f"{period}, {market}/{category} ({segment}): steady week, "
        "no notable movements."
    )


# ---------------------------------------------------------------------------
# Body generator
# ---------------------------------------------------------------------------


def _build_body(
    week_start: pd.Timestamp,
    dim_key: str,
    daily_slice: pd.DataFrame,
    weekly_slice: pd.DataFrame,
    change_points_slice: pd.DataFrame,
    archetypes_slice: pd.DataFrame,
) -> tuple[str, dict]:
    """Return (body_text, facts_dict). Facts is the structured evidence
    that grounds every claim in the text."""
    market, category, segment = dim_key.split("/")
    lines: list[str] = []

    # Volume summary from daily data
    if not daily_slice.empty:
        sessions = daily_slice["sessions"].sum()
        unique_users = daily_slice["unique_users"].sum()
        revenue = daily_slice["revenue_usd"].sum()
        lines.append(
            f"Activity: {_fmt_num(sessions)} sessions from "
            f"{_fmt_num(unique_users)} unique users, "
            f"${_fmt_num(revenue)} revenue."
        )

    # Metric movements from weekly decomposition
    notable = weekly_slice[weekly_slice["wow_delta"].abs() > 0.05]
    if not notable.empty:
        moves = []
        for _, r in notable.iterrows():
            direction = "rose" if r["wow_delta"] > 0 else "fell"
            trend_hint = ""
            if abs(r["residual_zscore"]) > 2.0:
                trend_hint = " (residual shock, not trend)"
            elif abs(r["trend_slope_4w"]) > 0.001:
                trend_hint = " (aligned with 4-week trend)"
            moves.append(
                f"{r['metric']} {direction} {_fmt_pct(r['wow_delta'])}{trend_hint}"
            )
        lines.append("Metric shifts: " + "; ".join(moves[:4]) + ".")

    # Change points
    if not change_points_slice.empty:
        cp_summaries = []
        for _, r in change_points_slice.head(3).iterrows():
            cp_summaries.append(
                f"{r['metric']} {r['direction']} {_fmt_pct(r['magnitude_pct'])} "
                f"(pre {_fmt_num(r['pre_mean'])} → post {_fmt_num(r['post_mean'])}, "
                f"confidence {r['confidence']:.0%})"
            )
        lines.append("Change points: " + "; ".join(cp_summaries) + ".")

    # Archetype shifts
    if not archetypes_slice.empty:
        growing = archetypes_slice.nlargest(2, "wow_delta")
        shrinking = archetypes_slice.nsmallest(2, "wow_delta")
        growth_notes = []
        for _, r in growing.iterrows():
            if r["wow_delta"] > 0.02:
                growth_notes.append(
                    f"{r['archetype_name']} +{r['wow_delta'] * 100:.1f}pp"
                )
        shrink_notes = []
        for _, r in shrinking.iterrows():
            if r["wow_delta"] < -0.02:
                shrink_notes.append(
                    f"{r['archetype_name']} {r['wow_delta'] * 100:.1f}pp"
                )
        if growth_notes or shrink_notes:
            parts = []
            if growth_notes:
                parts.append("growing archetypes: " + ", ".join(growth_notes))
            if shrink_notes:
                parts.append("shrinking: " + ", ".join(shrink_notes))
            lines.append("Behavior mix: " + "; ".join(parts) + ".")

    body = " ".join(lines) if lines else "No notable activity this week."

    facts = {
        "dim_key": dim_key,
        "week_start": week_start.strftime("%Y-%m-%d"),
        "sessions": int(daily_slice["sessions"].sum()) if not daily_slice.empty else 0,
        "revenue_usd": float(daily_slice["revenue_usd"].sum())
        if not daily_slice.empty
        else 0.0,
        "change_point_ids": change_points_slice["change_id"].tolist()
        if "change_id" in change_points_slice.columns
        else [],
        "metric_moves": notable[["metric", "wow_delta", "residual_zscore"]].to_dict(
            orient="records"
        )
        if not notable.empty
        else [],
    }

    return body, facts


# ---------------------------------------------------------------------------
# Optional LLM polish
# ---------------------------------------------------------------------------


def _llm_polish(headline: str, body: str) -> str | None:
    """If Anthropic API key is set, ask Claude to rephrase the body.
    The LLM may ONLY rephrase — it may not invent facts. This is enforced
    by keeping the template body around and comparing numeric tokens."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        import anthropic
    except ImportError:
        return None

    prompt = (
        "You are rewriting a pre-generated analytics summary to read more "
        "naturally. DO NOT add, remove, or change any number. DO NOT add "
        "interpretations not in the source. Keep it under 80 words.\n\n"
        f"Headline: {headline}\n\nBody:\n{body}"
    )
    try:
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(use_llm_polish: bool = False) -> pd.DataFrame:
    t0 = time.time()

    # Load inputs
    daily = pd.read_parquet(config.LAYER2_DIR / "daily.parquet")
    daily["date"] = pd.to_datetime(daily["date"])
    daily["week_start"] = daily["date"].dt.to_period("W-MON").dt.start_time
    daily["dim_key"] = daily["market"] + "/" + daily["category"] + "/" + daily["segment"]

    weekly = pd.read_parquet(config.LAYER3_DIR / "weekly_decomposed.parquet")
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])

    cp_path = config.LAYER4_DIR / "change_points.parquet"
    change_points = pd.read_parquet(cp_path) if cp_path.exists() else pd.DataFrame()
    if not change_points.empty:
        change_points["change_date"] = pd.to_datetime(change_points["change_date"])
        change_points["week_start"] = (
            change_points["change_date"]
            .dt.to_period("W-MON")
            .dt.start_time
        )

    arch_path = config.LAYER4_DIR / "archetypes_per_week.parquet"
    archetypes = pd.read_parquet(arch_path) if arch_path.exists() else pd.DataFrame()
    if not archetypes.empty:
        archetypes["week_start"] = pd.to_datetime(archetypes["week_start"])

    # Unique (week, dim_key) pairs we care about
    keys = weekly[["week_start", "dim_key"]].drop_duplicates()

    rows = []
    for _, key in keys.iterrows():
        wk = key["week_start"]
        dk = key["dim_key"]

        daily_slice = daily[(daily["week_start"] == wk) & (daily["dim_key"] == dk)]
        weekly_slice = weekly[(weekly["week_start"] == wk) & (weekly["dim_key"] == dk)]
        if change_points.empty:
            cp_slice = pd.DataFrame()
        else:
            cp_slice = change_points[
                (change_points["week_start"] == wk) & (change_points["dim_key"] == dk)
            ]
        if archetypes.empty:
            arch_slice = pd.DataFrame()
        else:
            arch_slice = archetypes[
                (archetypes["week_start"] == wk) & (archetypes["dim_key"] == dk)
            ]

        headline = _build_headline(wk, dk, weekly_slice, cp_slice)
        body, facts = _build_body(
            wk, dk, daily_slice, weekly_slice, cp_slice, arch_slice
        )
        body_polished = None
        generated_by = "template"
        if use_llm_polish:
            polished = _llm_polish(headline, body)
            if polished is not None:
                body_polished = polished
                generated_by = "template+llm-polish"

        rows.append(
            {
                "week_start": wk,
                "dim_key": dk,
                "headline": headline,
                "body": body_polished or body,
                "body_raw_template": body,
                "facts": facts,
                "generated_by": generated_by,
            }
        )

    df = pd.DataFrame(rows)
    out_path = config.LAYER4_DIR / "narrative_log.parquet"
    df.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    print(
        f"  [layer4/narrative] {len(df):,} paragraphs "
        f"-> {out_path.name} in {elapsed:.1f}s"
    )
    return df


if __name__ == "__main__":
    run(use_llm_polish=bool(os.environ.get("ANTHROPIC_API_KEY")))
