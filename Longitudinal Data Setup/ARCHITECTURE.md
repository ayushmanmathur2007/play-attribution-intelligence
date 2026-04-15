# Architecture — Longitudinal Data Setup

## Data flow

```
  ┌────────────────┐
  │ synthetic clk  │  generator/events.py
  │ persona-driven │  + initiative perturbations
  └────────┬───────┘
           │ ~1.6M events/18mo (prototype)
           ▼
  ╔═════════════════╗
  ║   LAYER 0       ║  data/raw/events_YYYYMM.parquet
  ║   raw events    ║  partition by month
  ╚════════┬════════╝
           │ layer1_sessionize.py
           │  · group by (user, session)
           │  · compact event sequence
           │  · roll up duration, outcomes
           ▼
  ╔═════════════════╗
  ║   LAYER 1       ║  data/layer1_sessions/sessions.parquet
  ║   sessions      ║  one row per user-session
  ╚════════┬════════╝
           │ layer2_aggregate.py
           │  · group by (date × dim_key)
           │  · count + sum + ratio metrics
           ▼
  ╔═════════════════╗
  ║   LAYER 2       ║  data/layer2_daily/daily.parquet
  ║   daily dim agg ║  date × market × category × segment
  ╚════════┬════════╝
           │ layer3_decompose.py
           │  · resample to weekly
           │  · STL(metric) -> trend + seasonal + residual
           │  · compute slopes, wow, yoy
           ▼
  ╔═════════════════╗
  ║   LAYER 3       ║  data/layer3_decomposed/weekly.parquet
  ║   weekly decomp ║  trend / seasonal / residual columns
  ╚════════┬════════╝
           │   ┌────────────────┬────────────────┬─────────────────┐
           ▼   ▼                ▼                ▼                 ▼
  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐
  │ change_points │  │  archetypes  │  │  embeddings  │  │ narrative_log │
  │ rolling-Z or  │  │  KMeans on   │  │  PCA over    │  │  template +   │
  │   ruptures    │  │ session-feat │  │  engineered  │  │  optional LLM │
  └───────┬───────┘  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘
          └──────────────────┴──────────────────┴──────────────────┘
                             │
                             ▼
                  ╔════════════════════╗
                  ║   LAYER 4          ║   data/layer4_ai_ready/*.parquet
                  ║   AI-ready store   ║   agent-native, pre-computed
                  ╚════════════════════╝
                             │
                             ▼
                     ┌───────────────┐
                     │ query/ai_ready│  Layer-4 client for the head-to-head demo
                     │   lookups     │
                     └───────────────┘
```

## Timing expectations on the prototype (default scale)

| Stage | Input rows | Output rows | Time (laptop) |
|---|---|---|---|
| Generate Layer 0 | — | ~1.6M events | ~40s |
| Layer 1 sessionize | 1.6M | ~140k | ~4s |
| Layer 2 aggregate | 140k | ~14k | ~1s |
| Layer 3 decompose | 14k | ~3k | ~5s |
| Layer 4 change points | 3k | ~50 | ~1s |
| Layer 4 archetypes | 140k sessions | ~50 clusters + 1.5k rows | ~3s |
| Layer 4 embeddings | 14k | ~1.5k vectors | ~1s |
| Layer 4 narrative | 1.5k | 1.5k paragraphs | ~2s (template-only) |
| **Total end-to-end** | | | **~60s** |

All numbers are for the default scale in `src/config.py`. Bump `NUM_USERS` or the date range to scale up.

## Query cost comparison (head-to-head demo)

Local DuckDB, real measurements:

| Path | Rows scanned | Wall time | Extrapolated to prod (100B events) |
|---|---|---|---|
| Naive raw | ~1.6M | ~1500ms | ~45min, ~$18 |
| Layer 4 lookup | ~30 | ~40ms | ~100ms, ~$0.003 |

Extrapolation uses a linear-in-rows scan model for naive (pessimistic but accurate for full-table queries) and a constant-time model for Layer 4 lookups (since the aggregate tables grow slowly in total size). See `src/query/naive_raw.py` and `src/query/ai_ready.py` for the math.

## Key design decisions

1. **DuckDB for everything** in the prototype. In production this would be BigQuery, but DuckDB lets us ship a laptop-runnable demo with identical SQL semantics.
2. **Parquet partitioning**. Layer 0 partitioned by month for realistic pruning. Layers 1–3 unpartitioned (small enough). Layer 4 files are read-all-at-once.
3. **STL period**. Chosen per metric: 52 weeks when history ≥ 104 weeks, else falls back to 13 weeks (quarterly), else to a centered moving average with no seasonal component.
4. **Change-point algorithm**. Default is a rolling-Z on the `residual` column from Layer 3 — fast, no extra deps. If `ruptures` is installed, `ruptures.Pelt` is used as the "hero" detector. Both results are persisted side-by-side for comparison.
5. **Narrative generator is template-first**. Numeric facts are always filled from Layer 2/3 — the LLM, if enabled, only rephrases. This is non-negotiable: agents consuming these narratives treat them as ground truth, so they must be grounded.

## Agent contract

What an attribution agent gets by reading Layer 4, in one trip:

```
def ask(question):
    dims = parse_dims(question)                                 # same as today
    cps   = query_change_points(metric, dims, window)           # ~5 rows
    narr  = query_narrative_log(dims, window)                   # ~10 paragraphs
    arch  = query_archetypes(dims, window)                      # ~10 rows
    emb   = query_embeddings(dims, window)                      # 2 vectors
    return llm_reason(question, cps, narr, arch, emb)
```

No `SELECT * FROM events WHERE ...`. No join across a year of clickstream. The agent's context is pre-curated history — it spends its reasoning budget on reasoning, not on counting.
