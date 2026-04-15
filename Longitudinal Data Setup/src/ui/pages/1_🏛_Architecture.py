"""Layer pyramid with live row counts + byte sizes.

This is the "show your work" page — the agent contract would be useless
without transparency into what got pre-computed and at what cost.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src import config  # noqa: E402

st.set_page_config(page_title="Architecture", page_icon="🏛", layout="wide")

st.title("🏛 The 5-layer pyramid")
st.caption(
    "Raw events collapse into ever-more-compressed, ever-more-semantic forms. "
    "Each layer is optional on its own — the magic is in having all five."
)


def _measure(path: Path) -> dict:
    if not path.exists():
        return {"rows": 0, "bytes": 0}
    try:
        import pyarrow.parquet as pq

        n = int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        try:
            n = int(len(pd.read_parquet(path)))
        except Exception:
            n = 0
    return {"rows": n, "bytes": path.stat().st_size}


# Gather
raw_files = list(config.RAW_DIR.glob("events_*.parquet"))
raw_rows = sum(_measure(p)["rows"] for p in raw_files)
raw_bytes = sum(_measure(p)["bytes"] for p in raw_files)

layers = [
    {
        "name": "Layer 0\nRaw events",
        "desc": "Every click, impression, install. One row per event.",
        "rows": raw_rows,
        "bytes": raw_bytes,
        "purpose": "Source of truth. Unqueryable at scale.",
    },
    {
        "name": "Layer 1\nSessions",
        "desc": "Events grouped into user sessions with feature flags.",
        "purpose": "Unit of human behavior. Enables archetype clustering.",
        **_measure(config.LAYER1_DIR / "sessions.parquet"),
    },
    {
        "name": "Layer 2\nDaily aggregates",
        "desc": "Counts + rates per (date × market × category × segment).",
        "purpose": "The dashboard layer. Queryable in milliseconds.",
        **_measure(config.LAYER2_DIR / "daily.parquet"),
    },
    {
        "name": "Layer 3\nWeekly decomposed",
        "desc": "STL trend/seasonal/residual + slope + z-score per metric.",
        "purpose": "Lets agents reason about 'surprise' without refitting STL.",
        **_measure(config.LAYER3_DIR / "weekly_decomposed.parquet"),
    },
    {
        "name": "Layer 4\nAI-ready",
        "desc": "Change points · archetypes · narrative log · embeddings · causal candidates.",
        "purpose": "The agent's primary interface. Paragraphs, not events.",
        "rows": sum(
            _measure(p)["rows"]
            for p in [
                config.LAYER4_DIR / "change_points.parquet",
                config.LAYER4_DIR / "archetypes_per_week.parquet",
                config.LAYER4_DIR / "narrative_log.parquet",
                config.LAYER4_DIR / "embeddings.parquet",
                config.LAYER4_DIR / "causal_candidates.parquet",
            ]
        ),
        "bytes": sum(
            _measure(p)["bytes"]
            for p in [
                config.LAYER4_DIR / "change_points.parquet",
                config.LAYER4_DIR / "archetypes_per_week.parquet",
                config.LAYER4_DIR / "narrative_log.parquet",
                config.LAYER4_DIR / "embeddings.parquet",
                config.LAYER4_DIR / "causal_candidates.parquet",
            ]
        ),
    },
]

# Metrics strip
cols = st.columns(len(layers))
for col, layer in zip(cols, layers):
    with col:
        st.metric(
            label=layer["name"].replace("\n", " · "),
            value=f"{layer['rows']:,} rows",
            delta=f"{layer['bytes'] / 1024 / 1024:.2f} MB",
            delta_color="off",
        )

st.divider()

# Pyramid visualization — widths proportional to log(bytes)
import math

max_bytes = max(l["bytes"] for l in layers) or 1
widths = [max(0.1, math.log10(max(l["bytes"], 1)) / math.log10(max_bytes)) for l in layers]

fig = go.Figure()
for i, (layer, w) in enumerate(zip(layers, widths)):
    y = len(layers) - i
    left = (1 - w) / 2
    right = left + w
    fig.add_shape(
        type="rect",
        x0=left,
        x1=right,
        y0=y - 0.4,
        y1=y + 0.4,
        line=dict(color="#6366f1"),
        fillcolor="rgba(99,102,241,0.25)",
    )
    fig.add_annotation(
        x=0.5,
        y=y,
        text=f"<b>{layer['name'].replace(chr(10), ' · ')}</b><br>"
        f"{layer['rows']:,} rows · {layer['bytes'] / 1024 / 1024:.2f} MB",
        showarrow=False,
        font=dict(size=11, color="#e2e8f0"),
    )

fig.update_layout(
    height=380,
    plot_bgcolor="#0f172a",
    paper_bgcolor="#0f172a",
    xaxis=dict(visible=False, range=[0, 1]),
    yaxis=dict(visible=False, range=[0, len(layers) + 1]),
    margin=dict(l=0, r=0, t=10, b=10),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# Per-layer details
st.subheader("Layer details")
for layer in layers:
    with st.container(border=True):
        cols = st.columns([2, 3, 2])
        cols[0].markdown(f"**{layer['name'].replace(chr(10), ' — ')}**")
        cols[1].write(layer["desc"])
        cols[2].caption(layer["purpose"])

st.divider()

# Agent contract — show a sample narrative
st.subheader("🗒 Sample narrative log row")
narr_path = config.LAYER4_DIR / "narrative_log.parquet"
if narr_path.exists():
    narr = pd.read_parquet(narr_path)
    if not narr.empty:
        sample = narr.sample(1, random_state=7).iloc[0]
        st.markdown(f"**Headline:** {sample['headline']}")
        st.markdown(f"**Body:** {sample['body']}")
        with st.expander("Grounded facts (what the agent can cite)"):
            st.json(sample.get("facts", {}))
    else:
        st.info("narrative_log.parquet is empty.")
else:
    st.info("narrative_log.parquet not generated yet.")
