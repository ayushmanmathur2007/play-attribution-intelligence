"""Reusable Plotly chart components for Play Attribution Intelligence."""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


# ---------------------------------------------------------------------------
# Shared layout defaults (dark-theme friendly)
# ---------------------------------------------------------------------------

_LAYOUT_DEFAULTS = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=50, b=40),
    font=dict(family="Inter, system-ui, sans-serif", size=13),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)

_COLORS = px.colors.qualitative.Set2


def _apply_defaults(fig: go.Figure, **overrides) -> go.Figure:
    """Apply shared layout defaults to a figure."""
    layout = {**_LAYOUT_DEFAULTS, **overrides}
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# Attribution pie chart
# ---------------------------------------------------------------------------

def attribution_pie_chart(attribution: list[dict]) -> go.Figure:
    """Create a donut chart showing attribution contribution percentages.

    Parameters
    ----------
    attribution : list[dict]
        Each dict should have at minimum ``cause`` (str) and
        ``contribution_pct`` (float 0-1).
    """
    if not attribution:
        fig = go.Figure()
        fig.add_annotation(text="No attribution data available", showarrow=False)
        return _apply_defaults(fig, title="Attribution Breakdown")

    labels = [a.get("cause", a.get("name", "Unknown")) for a in attribution]
    raw_values = [abs(float(a.get("contribution_pct", a.get("contribution", 0)) or 0)) for a in attribution]
    # Auto-detect scale: if all values <= 1.0, treat as 0-1 and multiply by 100
    if all(v <= 1.0 for v in raw_values) and any(v > 0 for v in raw_values):
        values = [v * 100 for v in raw_values]
    else:
        values = raw_values
    confidence = [a.get("confidence", "medium") for a in attribution]

    # Color by confidence: high=green-ish, medium=amber, low=red-ish
    confidence_colors = {
        "high": "#22c55e",
        "medium": "#f59e0b",
        "low": "#ef4444",
    }
    colors = [confidence_colors.get(c, "#94a3b8") for c in confidence]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.45,
                marker=dict(colors=colors, line=dict(color="#1e293b", width=2)),
                textinfo="label+percent",
                textposition="outside",
                hovertemplate=(
                    "<b>%{label}</b><br>"
                    "Contribution: %{value:.1f}%<br>"
                    "<extra></extra>"
                ),
            )
        ]
    )

    fig.add_annotation(
        text="Attribution",
        x=0.5, y=0.5,
        font=dict(size=16, color="#e2e8f0"),
        showarrow=False,
    )

    return _apply_defaults(fig, title="Attribution Breakdown", showlegend=False)


# ---------------------------------------------------------------------------
# Metric trend chart
# ---------------------------------------------------------------------------

def metric_trend_chart(
    df: pd.DataFrame,
    metric: str,
    markets: list[str] | None = None,
    initiatives_df: pd.DataFrame | None = None,
    change_points_df: pd.DataFrame | None = None,
    height: int = 450,
) -> go.Figure:
    """Create a time series line chart with optional initiative overlays.

    Parameters
    ----------
    df : pd.DataFrame
        Must have columns ``date``, ``value``, ``market_id``, ``metric_name``.
    metric : str
        The metric_name to filter on.
    markets : list[str] | None
        Markets to include. If None, shows all.
    initiatives_df : pd.DataFrame | None
        Initiative calendar with ``start_date``, ``end_date``, ``initiative_name``.
    change_points_df : pd.DataFrame | None
        Change points with ``date``, ``metric_name``.
    """
    fig = go.Figure()

    if df.empty:
        fig.add_annotation(text="No metric data available", showarrow=False)
        return _apply_defaults(fig, title=f"{metric} — Trend", height=height)

    plot_df = df.copy()
    plot_df["date"] = pd.to_datetime(plot_df["date"])

    # Filter to the requested metric
    if "metric_name" in plot_df.columns:
        plot_df = plot_df[plot_df["metric_name"] == metric]

    if plot_df.empty:
        fig.add_annotation(text=f"No data for metric '{metric}'", showarrow=False)
        return _apply_defaults(fig, title=f"{metric} — Trend", height=height)

    # Filter markets
    if markets and "market_id" in plot_df.columns:
        plot_df = plot_df[plot_df["market_id"].isin(markets)]

    # Group by market
    if "market_id" in plot_df.columns:
        grouped = plot_df.groupby("market_id")
    else:
        grouped = [("All", plot_df)]

    for i, (market, group) in enumerate(grouped):
        group = group.sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=group["date"],
                y=group["value"],
                mode="lines+markers",
                name=str(market),
                marker=dict(size=4),
                line=dict(color=_COLORS[i % len(_COLORS)], width=2),
                hovertemplate=(
                    f"<b>{market}</b><br>"
                    "Date: %{x|%Y-%m-%d}<br>"
                    "Value: %{y:,.4f}<br>"
                    "<extra></extra>"
                ),
            )
        )

    # Initiative overlays as colored bands
    if initiatives_df is not None and not initiatives_df.empty:
        init_df = initiatives_df.copy()
        init_df["start_date"] = pd.to_datetime(init_df["start_date"])
        init_df["end_date"] = pd.to_datetime(init_df["end_date"])

        for idx, row in init_df.iterrows():
            name = row.get("initiative_name", row.get("name", f"Initiative {idx}"))
            fig.add_vrect(
                x0=row["start_date"],
                x1=row["end_date"],
                fillcolor=_COLORS[(idx + 3) % len(_COLORS)],
                opacity=0.12,
                line_width=0,
                annotation_text=name,
                annotation_position="top left",
                annotation_font_size=10,
                annotation_font_color="#94a3b8",
            )

    # Change point markers as vertical dashed lines
    if change_points_df is not None and not change_points_df.empty:
        cp_df = change_points_df.copy()
        cp_df["date"] = pd.to_datetime(cp_df["date"])

        # Filter to the metric if column exists
        if "metric_name" in cp_df.columns:
            cp_df = cp_df[cp_df["metric_name"] == metric]

        for _, row in cp_df.iterrows():
            fig.add_vline(
                x=row["date"],
                line_dash="dash",
                line_color="#f97316",
                line_width=1.5,
                annotation_text="CP",
                annotation_font_size=10,
                annotation_font_color="#f97316",
            )

    readable = metric.replace("_", " ").title()
    return _apply_defaults(fig, title=f"{readable} -- Trend", height=height)


# ---------------------------------------------------------------------------
# Multi-metric overlay chart
# ---------------------------------------------------------------------------

def multi_metric_chart(
    df: pd.DataFrame,
    metrics: list[str],
    market: str | None = None,
    height: int = 450,
) -> go.Figure:
    """Overlay multiple metrics on the same chart with dual y-axes if needed.

    Uses a secondary y-axis when metrics have very different scales.
    """
    fig = go.Figure()

    if df.empty or not metrics:
        fig.add_annotation(text="No data available", showarrow=False)
        return _apply_defaults(fig, title="Multi-Metric Comparison", height=height)

    plot_df = df.copy()
    plot_df["date"] = pd.to_datetime(plot_df["date"])
    if market and "market_id" in plot_df.columns:
        plot_df = plot_df[plot_df["market_id"] == market]

    use_secondary = len(metrics) == 2

    for i, m in enumerate(metrics):
        m_df = plot_df[plot_df["metric_name"] == m].sort_values("date")
        if m_df.empty:
            continue
        yaxis = "y2" if (i == 1 and use_secondary) else "y"
        fig.add_trace(
            go.Scatter(
                x=m_df["date"],
                y=m_df["value"],
                mode="lines",
                name=m.replace("_", " ").title(),
                line=dict(color=_COLORS[i % len(_COLORS)], width=2),
                yaxis=yaxis,
            )
        )

    layout_extra: dict = {"height": height, "title": "Multi-Metric Comparison"}
    if use_secondary:
        layout_extra["yaxis2"] = dict(
            title=metrics[1].replace("_", " ").title(),
            overlaying="y",
            side="right",
            showgrid=False,
        )
        layout_extra["yaxis"] = dict(title=metrics[0].replace("_", " ").title())

    return _apply_defaults(fig, **layout_extra)


# ---------------------------------------------------------------------------
# Eval radar / spider chart
# ---------------------------------------------------------------------------

def score_radar_chart(scores: dict, max_score: float = 5.0) -> go.Figure:
    """Create a radar/spider chart for eval dimension scores.

    Parameters
    ----------
    scores : dict
        Mapping of dimension name -> score (numeric).
    max_score : float
        Maximum possible score (used for the radial axis range).
    """
    if not scores:
        fig = go.Figure()
        fig.add_annotation(text="No eval scores available", showarrow=False)
        return _apply_defaults(fig, title="Eval Dimension Scores")

    categories = list(scores.keys())
    values = [scores[k] if isinstance(scores[k], (int, float)) else scores[k].get("score", 0) for k in categories]

    # Close the polygon
    categories_closed = categories + [categories[0]]
    values_closed = values + [values[0]]

    fig = go.Figure(
        data=[
            go.Scatterpolar(
                r=values_closed,
                theta=categories_closed,
                fill="toself",
                fillcolor="rgba(99, 102, 241, 0.25)",
                line=dict(color="#6366f1", width=2),
                marker=dict(size=6, color="#6366f1"),
                name="Score",
            )
        ]
    )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max_score],
                tickfont=dict(size=11),
            ),
            bgcolor="rgba(0,0,0,0)",
        ),
    )

    return _apply_defaults(fig, title="Eval Dimension Scores", showlegend=False)


# ---------------------------------------------------------------------------
# Eval difficulty bar chart
# ---------------------------------------------------------------------------

def difficulty_bar_chart(results: list[dict]) -> go.Figure:
    """Grouped bar chart of scores by difficulty level.

    Parameters
    ----------
    results : list[dict]
        Each dict should have ``difficulty`` and score dimension keys
        (clarity, actionability, completeness, tone, structure).
    """
    if not results:
        fig = go.Figure()
        fig.add_annotation(text="No eval results available", showarrow=False)
        return _apply_defaults(fig, title="Scores by Difficulty")

    df = pd.DataFrame(results)

    score_dims = ["clarity", "actionability", "completeness", "tone", "structure"]
    available_dims = [d for d in score_dims if d in df.columns]

    if not available_dims or "difficulty" not in df.columns:
        fig = go.Figure()
        fig.add_annotation(text="Missing difficulty or score columns", showarrow=False)
        return _apply_defaults(fig, title="Scores by Difficulty")

    # Extract numeric scores (handle nested dicts with 'score' key)
    for dim in available_dims:
        df[dim] = df[dim].apply(lambda x: x.get("score", x) if isinstance(x, dict) else x)

    grouped = df.groupby("difficulty")[available_dims].mean().reset_index()

    fig = go.Figure()
    for i, dim in enumerate(available_dims):
        fig.add_trace(
            go.Bar(
                x=grouped["difficulty"],
                y=grouped[dim],
                name=dim.title(),
                marker_color=_COLORS[i % len(_COLORS)],
            )
        )

    fig.update_layout(barmode="group")
    return _apply_defaults(fig, title="Average Scores by Difficulty Level", height=400)


# ---------------------------------------------------------------------------
# Confidence badge helper (for use in markdown)
# ---------------------------------------------------------------------------

def confidence_badge(level: str) -> str:
    """Return a colored markdown badge string for confidence level."""
    colors = {
        "high": ":green[HIGH]",
        "medium": ":orange[MEDIUM]",
        "low": ":red[LOW]",
    }
    return colors.get(level.lower(), f":gray[{level.upper()}]")
