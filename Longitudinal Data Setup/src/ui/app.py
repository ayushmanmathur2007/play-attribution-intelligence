"""Streamlit entrypoint for the Longitudinal Data Setup demo.

Run with:
    streamlit run src/ui/app.py

Uses Streamlit's multipage convention — files in `src/ui/pages/` show up
in the sidebar automatically.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Make `src.*` importable when Streamlit runs this as a plain script
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src import config  # noqa: E402

st.set_page_config(
    page_title="Longitudinal Data Setup",
    page_icon="🗼",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _parquet_rows(path: Path) -> int:
    """Cheap row count via parquet footer — no data read."""
    try:
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        try:
            return int(len(pd.read_parquet(path)))
        except Exception:
            return 0


LAYERS = [
    ("Layer 0 — raw events", config.RAW_DIR.glob("events_*.parquet")),
    ("Layer 1 — sessions", [config.LAYER1_DIR / "sessions.parquet"]),
    ("Layer 2 — daily aggregates", [config.LAYER2_DIR / "daily.parquet"]),
    ("Layer 3 — weekly decomposed", [config.LAYER3_DIR / "weekly_decomposed.parquet"]),
    ("Layer 4 — change points", [config.LAYER4_DIR / "change_points.parquet"]),
    ("Layer 4 — archetypes", [config.LAYER4_DIR / "archetypes_per_week.parquet"]),
    ("Layer 4 — narrative log", [config.LAYER4_DIR / "narrative_log.parquet"]),
    ("Layer 4 — embeddings", [config.LAYER4_DIR / "embeddings.parquet"]),
    ("Layer 4 — causal candidates", [config.LAYER4_DIR / "causal_candidates.parquet"]),
]


def main() -> None:
    st.title("🗼 Longitudinal Data Setup")
    st.caption(
        "Pre-digested institutional memory for AI agents. "
        "Agents read **paragraphs**, not event streams."
    )

    st.markdown(
        """
### Why this exists

Raw clickstream at Play scale is ~100B events/day. No longitudinal question
("What moved in India/Games last November and why?") can be answered by
scanning that. Humans work around it with hand-built dashboards. Agents
need the same pre-digestion — but shaped for their eyes, not ours.

This prototype builds a **5-layer aggregation pyramid** that turns raw events
into ~52 paragraph-per-week summaries per (market × category × segment).
An agent answering a 12-month question reads ~50 paragraphs instead of
billions of rows.

**Open a page in the sidebar:**
- **Architecture** — the 5-layer pyramid with live byte counts.
- **Head-to-head** — same questions, answered two ways: raw scan vs layer-4 lookup.
        """.strip()
    )

    st.divider()
    st.subheader("System status")

    rows = []
    for name, paths in LAYERS:
        paths = list(paths)
        total_rows = 0
        total_bytes = 0.0
        present = 0
        for p in paths:
            if p.exists():
                present += 1
                total_bytes += p.stat().st_size
                total_rows += _parquet_rows(p)
        rows.append(
            {
                "layer": name,
                "files": f"{present}/{len(paths) if paths else '?'}",
                "rows": f"{total_rows:,}" if total_rows else "—",
                "size_mb": f"{total_bytes / 1024 / 1024:.2f}",
                "status": "✅ ready" if present and total_rows else "⚠️ missing",
            }
        )

    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    st.info(
        "If any layer shows ⚠️, run:  "
        "`python -m src.generator.run && python -m src.pipeline.build`  "
        "from the project root.",
        icon="ℹ️",
    )


main()
