"""Metric card component for KPI display."""

from __future__ import annotations

import streamlit as st


def metric_card(
    label: str,
    value: float,
    fmt: str = ".2f",
    delta: float | None = None,
    threshold: float | None = None,
    prefix: str = "",
    suffix: str = "",
    help_text: str | None = None,
):
    """Display a KPI metric card with optional delta and pass/fail coloring.

    Parameters
    ----------
    label : str
        Metric label shown above the value.
    value : float
        Numeric value to display.
    fmt : str
        Format spec for the value (e.g. ".2f", ".0f", ".1%").
    delta : float | None
        Optional delta value to show change.
    threshold : float | None
        If provided, colors the card green when value >= threshold,
        red when below.
    prefix : str
        Prefix string (e.g. "$").
    suffix : str
        Suffix string (e.g. "%", " pts").
    help_text : str | None
        Tooltip text on hover.
    """
    # Format value
    formatted_value = f"{prefix}{value:{fmt}}{suffix}"

    # Format delta
    delta_str: str | None = None
    delta_color: str | None = None
    if delta is not None:
        delta_str = f"{delta:+{fmt}}{suffix}"
        delta_color = "normal"

    # Use st.metric for the core display
    st.metric(
        label=label,
        value=formatted_value,
        delta=delta_str,
        delta_color=delta_color,
        help=help_text,
    )

    # Show pass/fail indicator below if threshold is set
    if threshold is not None:
        if value >= threshold:
            st.caption(f":green[PASS] (threshold: {prefix}{threshold:{fmt}}{suffix})")
        else:
            st.caption(f":red[FAIL] (threshold: {prefix}{threshold:{fmt}}{suffix})")


def metric_card_row(
    metrics: list[dict],
    columns: int | None = None,
):
    """Render a row of metric cards.

    Parameters
    ----------
    metrics : list[dict]
        Each dict is passed as kwargs to ``metric_card()``.
        Required keys: ``label``, ``value``.
    columns : int | None
        Number of columns. Defaults to len(metrics) or 6, whichever is smaller.
    """
    if not metrics:
        return

    n_cols = columns or min(len(metrics), 6)
    cols = st.columns(n_cols)
    for i, m in enumerate(metrics):
        with cols[i % n_cols]:
            metric_card(**m)
