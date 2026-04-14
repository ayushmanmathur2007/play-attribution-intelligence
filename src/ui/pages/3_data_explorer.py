"""Page 3: Data Explorer -- interactive exploration of synthetic data."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "synthetic"
CONFIG_DIR = PROJECT_ROOT / "config"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Data Explorer | Play Attribution Intelligence",
    page_icon="📈",
    layout="wide",
)

st.title("Data Explorer")
st.markdown(
    "Interactively explore the synthetic dataset -- metrics, initiatives, "
    "and change points across markets and categories."
)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_dimensions_config() -> dict:
    """Load dimensions.yaml for metric/market/category definitions."""
    path = CONFIG_DIR / "dimensions.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


@st.cache_data
def load_daily_metrics() -> pd.DataFrame:
    """Load the daily_metrics.parquet file."""
    path = DATA_DIR / "daily_metrics.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def load_initiative_calendar() -> pd.DataFrame:
    """Load the initiative_calendar.csv file."""
    path = DATA_DIR / "initiative_calendar.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"])
    if "end_date" in df.columns:
        df["end_date"] = pd.to_datetime(df["end_date"])
    return df


@st.cache_data
def load_change_points() -> pd.DataFrame:
    """Load the change_points.csv file."""
    path = DATA_DIR / "change_points.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data
def load_confounder_log() -> pd.DataFrame:
    """Load confounder_log.csv if it exists."""
    path = DATA_DIR / "confounder_log.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"])
    if "end_date" in df.columns:
        df["end_date"] = pd.to_datetime(df["end_date"])
    return df


@st.cache_data
def load_offer_catalog() -> pd.DataFrame:
    """Load offer_catalog.csv if it exists."""
    path = DATA_DIR / "offer_catalog.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

dims_config = load_dimensions_config()
daily_df = load_daily_metrics()
init_df = load_initiative_calendar()
cp_df = load_change_points()

# Check if data exists
if daily_df.empty:
    st.warning("No synthetic data found. Generate it first:")
    st.code(
        "python -m src.data_generator.generator",
        language="bash",
    )
    st.stop()

# ---------------------------------------------------------------------------
# Build lookup tables from config
# ---------------------------------------------------------------------------

metric_defs = {m["id"]: m for m in dims_config.get("metrics", [])}
all_metrics = sorted(metric_defs.keys()) if metric_defs else sorted(daily_df["metric_name"].unique().tolist())
metric_names = {m["id"]: m.get("name", m["id"]) for m in dims_config.get("metrics", [])}

market_defs = {m["id"]: m for m in dims_config.get("markets", [])}
all_markets = sorted(market_defs.keys()) if market_defs else sorted(daily_df["market_id"].unique().tolist())
market_names = {m["id"]: m.get("name", m["id"]) for m in dims_config.get("markets", [])}

category_defs = {c["id"]: c for c in dims_config.get("categories", [])}
all_categories = sorted(category_defs.keys()) if category_defs else sorted(daily_df["category_id"].unique().tolist())
category_names = {c["id"]: c.get("name", c["id"]) for c in dims_config.get("categories", [])}

# Default top 5 markets (Tier 1)
default_markets = [m["id"] for m in dims_config.get("markets", []) if m.get("tier") == 1][:5]
if not default_markets:
    default_markets = all_markets[:5]

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.markdown("### Filters")

# Metric selector
default_metric_idx = 0
if "offer_redemption_rate" in all_metrics:
    default_metric_idx = all_metrics.index("offer_redemption_rate")

selected_metric = st.sidebar.selectbox(
    "Metric",
    options=all_metrics,
    format_func=lambda x: f"{metric_names.get(x, x)} ({x})",
    index=default_metric_idx,
)

# Show metric definition
if selected_metric in metric_defs:
    mdef = metric_defs[selected_metric]
    with st.sidebar.expander("Metric Definition", expanded=False):
        st.markdown(f"**{mdef.get('name', selected_metric)}**")
        st.markdown(f"**Unit:** {mdef.get('unit', 'N/A')}")
        definition = mdef.get("definition", "")
        if definition:
            st.caption(definition.strip()[:300])
        biz_logic = mdef.get("business_logic", "")
        if biz_logic:
            st.markdown(f"**Logic:** {biz_logic.strip()[:200]}")
        caveats = mdef.get("caveats", "")
        if caveats:
            st.markdown(f"**Caveats:** {caveats.strip()[:200]}")

# Market selector
selected_markets = st.sidebar.multiselect(
    "Markets",
    options=all_markets,
    default=default_markets[:3],
    format_func=lambda x: f"{market_names.get(x, x)} ({x})",
)

# Category selector
selected_categories = st.sidebar.multiselect(
    "Categories (leave empty = all)",
    options=all_categories,
    default=[],
    format_func=lambda x: f"{category_names.get(x, x)} ({x})",
)

# Date range
min_date = daily_df["date"].min().date()
max_date = daily_df["date"].max().date()

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Ensure we have start and end dates
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# Overlay options
st.sidebar.markdown("### Overlays")
show_initiatives = st.sidebar.checkbox("Show initiatives", value=True)
show_change_points = st.sidebar.checkbox("Show change points", value=True)

# Comparison mode
st.sidebar.markdown("### Comparison")
comparison_metrics = st.sidebar.multiselect(
    "Overlay additional metrics",
    options=[m for m in all_metrics if m != selected_metric],
    default=[],
    format_func=lambda x: metric_names.get(x, x),
    max_selections=3,
)

# Aggregation
agg_by = st.sidebar.radio(
    "Aggregate categories by",
    options=["Average across categories", "Show separately"],
    index=0,
)

# ---------------------------------------------------------------------------
# Filter data
# ---------------------------------------------------------------------------

filtered_df = daily_df[
    (daily_df["metric_name"] == selected_metric)
    & (daily_df["market_id"].isin(selected_markets))
    & (daily_df["date"] >= pd.Timestamp(start_date))
    & (daily_df["date"] <= pd.Timestamp(end_date))
].copy()

if selected_categories:
    filtered_df = filtered_df[filtered_df["category_id"].isin(selected_categories)]

# Aggregate across categories and segments for a cleaner chart
if agg_by == "Average across categories":
    filtered_df = (
        filtered_df.groupby(["date", "market_id", "metric_name"])["value"]
        .mean()
        .reset_index()
    )
else:
    # Aggregate segments but keep categories separate
    if "segment_id" in filtered_df.columns:
        filtered_df = (
            filtered_df.groupby(["date", "market_id", "category_id", "metric_name"])["value"]
            .mean()
            .reset_index()
        )

# Filter initiatives and change points for the period
filtered_init = pd.DataFrame()
if show_initiatives and not init_df.empty:
    filtered_init = init_df[
        (init_df["start_date"] <= pd.Timestamp(end_date))
        & (init_df["end_date"] >= pd.Timestamp(start_date))
    ]
    # Further filter by market if possible
    if selected_markets and "market_id" in filtered_init.columns:
        filtered_init = filtered_init[
            filtered_init["market_id"].apply(
                lambda v: pd.isna(v) or str(v).upper() == "ALL"
                or any(m in str(v) for m in selected_markets)
            )
        ]

filtered_cp = pd.DataFrame()
if show_change_points and not cp_df.empty:
    filtered_cp = cp_df[
        (cp_df["date"] >= pd.Timestamp(start_date))
        & (cp_df["date"] <= pd.Timestamp(end_date))
        & (cp_df["metric_name"] == selected_metric)
    ]
    if selected_markets and "market_id" in filtered_cp.columns:
        filtered_cp = filtered_cp[filtered_cp["market_id"].isin(selected_markets)]

# ---------------------------------------------------------------------------
# Main chart
# ---------------------------------------------------------------------------

st.markdown(f"### {metric_names.get(selected_metric, selected_metric)} -- Trend")

if filtered_df.empty:
    st.info("No data matches the current filters. Try widening the date range or selecting more markets.")
else:
    from src.ui.components.charts import metric_trend_chart

    fig = metric_trend_chart(
        df=filtered_df,
        metric=selected_metric,
        markets=selected_markets,
        initiatives_df=filtered_init if show_initiatives else None,
        change_points_df=filtered_cp if show_change_points else None,
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary stats
    st.markdown("#### Summary Statistics")
    stats_cols = st.columns(5)
    with stats_cols[0]:
        st.metric("Mean", f"{filtered_df['value'].mean():,.4f}")
    with stats_cols[1]:
        st.metric("Median", f"{filtered_df['value'].median():,.4f}")
    with stats_cols[2]:
        st.metric("Std Dev", f"{filtered_df['value'].std():,.4f}")
    with stats_cols[3]:
        st.metric("Min", f"{filtered_df['value'].min():,.4f}")
    with stats_cols[4]:
        st.metric("Max", f"{filtered_df['value'].max():,.4f}")

# ---------------------------------------------------------------------------
# Comparison chart (multi-metric overlay)
# ---------------------------------------------------------------------------

if comparison_metrics:
    st.markdown("---")
    st.markdown("### Multi-Metric Comparison")

    comp_metrics = [selected_metric] + comparison_metrics
    comp_df = daily_df[
        (daily_df["metric_name"].isin(comp_metrics))
        & (daily_df["market_id"].isin(selected_markets))
        & (daily_df["date"] >= pd.Timestamp(start_date))
        & (daily_df["date"] <= pd.Timestamp(end_date))
    ]

    if selected_categories:
        comp_df = comp_df[comp_df["category_id"].isin(selected_categories)]

    # Aggregate for clean comparison
    comp_df = (
        comp_df.groupby(["date", "metric_name"])["value"]
        .mean()
        .reset_index()
    )

    if not comp_df.empty:
        from src.ui.components.charts import multi_metric_chart

        comp_market = selected_markets[0] if len(selected_markets) == 1 else None
        fig = multi_metric_chart(comp_df, comp_metrics, market=comp_market, height=450)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No comparison data available for the selected filters.")

# ---------------------------------------------------------------------------
# Initiative Timeline
# ---------------------------------------------------------------------------

if show_initiatives and not filtered_init.empty:
    st.markdown("---")
    st.markdown("### Active Initiatives")

    init_display = filtered_init.copy()
    # Select display columns that exist
    preferred_cols = [
        "initiative_name", "name", "initiative_id", "id", "type",
        "start_date", "end_date", "market_id", "category_id",
        "description", "status", "expected_impact_pct",
    ]
    display_cols = [c for c in preferred_cols if c in init_display.columns]
    if display_cols:
        st.dataframe(
            init_display[display_cols],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.dataframe(init_display, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Change Points
# ---------------------------------------------------------------------------

if show_change_points and not filtered_cp.empty:
    st.markdown("---")
    st.markdown("### Detected Change Points")

    cp_display = filtered_cp.copy()
    preferred_cols = [
        "date", "metric_name", "market_id", "category_id",
        "direction", "magnitude", "confidence", "description",
    ]
    display_cols = [c for c in preferred_cols if c in cp_display.columns]
    if display_cols:
        st.dataframe(
            cp_display[display_cols],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.dataframe(cp_display, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Raw data viewer
# ---------------------------------------------------------------------------

st.markdown("---")
with st.expander("Raw Data (filtered)", expanded=False):
    tab1, tab2, tab3 = st.tabs(["Daily Metrics", "Initiatives", "Change Points"])

    with tab1:
        if not filtered_df.empty:
            st.dataframe(
                filtered_df.head(500),
                use_container_width=True,
                height=400,
            )
            st.caption(f"Showing first 500 of {len(filtered_df)} rows")
        else:
            st.info("No metric data for current filters.")

    with tab2:
        if not init_df.empty:
            st.dataframe(init_df, use_container_width=True, height=400)
        else:
            st.info("No initiative calendar data.")

    with tab3:
        if not cp_df.empty:
            st.dataframe(cp_df, use_container_width=True, height=400)
        else:
            st.info("No change point data.")

# ---------------------------------------------------------------------------
# Additional data tables
# ---------------------------------------------------------------------------

with st.expander("Additional Data Files"):
    conf_df = load_confounder_log()
    offer_df = load_offer_catalog()

    tab_a, tab_b = st.tabs(["Confounder Log", "Offer Catalog"])

    with tab_a:
        if not conf_df.empty:
            st.dataframe(conf_df, use_container_width=True, height=300)
        else:
            st.info("No confounder_log.csv found.")

    with tab_b:
        if not offer_df.empty:
            st.dataframe(offer_df, use_container_width=True, height=300)
        else:
            st.info("No offer_catalog.csv found.")
