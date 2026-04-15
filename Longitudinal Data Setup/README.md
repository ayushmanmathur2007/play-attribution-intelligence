# Longitudinal Data Setup

**An AI-ready data layer that lets attribution agents reason over a year of clickstream in milliseconds instead of hours.**

This folder is a standalone prototype: synthetic Play-style clickstream + a 5-layer build pipeline + a head-to-head Streamlit demo that shows why shaping data for agents (not dashboards) unlocks longitudinal reasoning.

Designed for the Google Play DS&A workshop. Companion piece to the main attribution agent demo in `../src/`.

---

## The one-paragraph version

Agents fail on "12-month trend" questions not because LLMs are bad at reasoning, but because they're being asked to reason over raw clickstream via SQL. At Google Play scale that's petabytes per query. The fix is a 5-layer aggregation pyramid capped with **four AI-native materializations** — pre-decomposed trends, materialized change points, compressed journey archetypes, and semantic event logs. Once that layer exists, the same agent becomes dramatically more capable without changing a single prompt.

This prototype builds that layer from synthetic data and runs the same longitudinal question two ways side-by-side: raw scan vs. AI-ready lookup. The contrast is meant to be visceral.

---

## Quick start

```bash
# From the repo root:
cd "Longitudinal Data Setup"

# Install deps (additive to the main project's requirements)
pip install -r requirements.txt

# Generate synthetic clickstream and build all 5 layers (takes ~60s)
./scripts/bootstrap.sh

# Launch the demo
streamlit run src/ui/app.py
```

Then open `http://localhost:8501`.

The first page shows the pyramid with live row counts and storage sizes from your just-built data. The second page is the head-to-head: type a longitudinal question, hit run, and watch the raw path and the Layer-4 path race each other on real local data with extrapolated production costs.

---

## What gets built

| Layer | Contents | Row count (default scale) | Built by |
|---|---|---|---|
| 0 · raw events | every click/search/install/offer/purchase | ~1.6M | `src/generator/` |
| 1 · sessions | one row per user-session | ~140k | `pipeline/layer1_sessionize.py` |
| 2 · daily aggregates | date × market × category × segment | ~14k | `pipeline/layer2_aggregate.py` |
| 3 · decomposed weekly | STL(trend + seasonal + residual) per metric | ~3k | `pipeline/layer3_decompose.py` |
| 4 · change_points | every detected shift | ~50 | `pipeline/change_points.py` |
| 4 · archetypes | KMeans clusters + weekly share | ~1.5k | `pipeline/archetypes.py` |
| 4 · embeddings | 8-dim PCA per (dim × week) | ~1.5k | `pipeline/embeddings.py` |
| 4 · narrative_log | one paragraph per (dim × week) | ~1.5k | `pipeline/narrative.py` |

The pyramid compresses by ~1000× at each step. At each layer the agent gains speed; at layer 4 it gains **reasoning shape** — data pre-digested into the things agents actually want to know.

---

## Why the numbers matter

The prototype runs on ~1.6M events. Production Google Play runs on ~100B/day. The head-to-head demo extrapolates: it runs the query for real locally, measures rows scanned and wall time, then scales the numbers to production volume and displays what the same question would cost against raw events.

A typical longitudinal query in the demo:
```
Raw scan          — 1.8s local → ~47 min at production scale — ~$18
Layer-4 lookup    — 42ms local → ~100ms at production scale — ~$0.003
```

That's three orders of magnitude per dimension. The point isn't the exact number; it's that the architecture bends the curve.

---

## File map

```
PRD.md                  — full product requirements
ARCHITECTURE.md         — diagrams + data contracts
scripts/bootstrap.sh    — one-command build
src/
├── config.py           — scale knobs, initiatives, paths
├── generator/          — synthetic clickstream
├── pipeline/           — 5 layers + 4 AI-native materializations
├── query/              — naive vs ai_ready query clients
└── ui/                 — Streamlit app (2 pages)
data/                   — generated, gitignored
```

Start with `PRD.md` if you want the pitch. Start with `src/ui/app.py` if you want to see it run.

---

## Relationship to the main attribution agent

This is the **data layer** underneath the agent you already saw in the main `play-attribution-intelligence` app. The attribution agent answers "why did this metric move." This layer answers "how does the agent see a year of history at once, without scanning a year of events." Together: a data architecture that makes every agent better, not just one agent that's slightly cleverer.

---

## Status

v0.1 — prototype. See `PRD.md` §11 for the phased rollout and §13 for open decisions flagged for the workshop kickoff.
