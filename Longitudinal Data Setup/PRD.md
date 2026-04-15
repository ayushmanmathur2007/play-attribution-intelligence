# PRD — Longitudinal Data Layer for AI Attribution

**Status:** Draft v0.1 — prototype scope
**Owner:** LatentView Analytics (Ayushman Mathur)
**Stakeholder:** Google Play DS&A
**Date:** April 2026
**One-line framing:** *An AI-ready data layer that lets agents reason over a year of clickstream in milliseconds instead of hours.*

---

## 1. Executive summary

Google Play's DS&A org has two pains that look separate but are the same problem wearing two hats:

1. **Data engineering pain** — raw clickstream is hundreds of billions of events per day. Longitudinal queries scan petabytes, SQL hits the ceiling, daily query jobs fail. Analysts work around it with sampling and short windows.
2. **AI pain** — attribution and forecasting agents fail on any question that spans more than a few weeks. They're asked to reason over data that was never shaped for reasoning, so they write expensive SQL, time out, or hallucinate.

**Our claim:** the agents aren't bad — the data layer underneath them was built for dashboards, not for reasoning. A 5-layer aggregation pyramid, topped with four AI-native materializations (trend decomposition, change-point detection, journey archetypes, and semantic event logs), turns a year of clickstream into something an agent can read in one breath.

This PRD specifies that layer. The prototype in `Longitudinal Data Setup/` is the proof — synthetic Play-style clickstream, full 5-layer build pipeline, and a head-to-head demo that runs the same longitudinal question two ways (raw scan vs. AI-ready lookup) and shows the latency, cost, and answer-quality delta side-by-side.

---

## 2. Problem statement

### 2.1 What agents are being asked to do
> *"Why did D7 retention in India Games drop 12% between Q2 and Q4?"*
> *"Compare the behavioral profile of offer-engaged users in October vs. the year before."*
> *"When did the shift in install-to-launch conversion start, and what correlates with it?"*

All three require reasoning across **months to years** of user behavior, segmented by multiple dimensions. All three are questions a human analyst answers in ~1 hour with the right pre-aggregates and in ~1 week without them.

### 2.2 Why the current stack fails them
| Symptom | Root cause |
|---|---|
| Agent SQL times out on 12-month queries | Raw events table has no dimensional pre-aggregation beyond day + market |
| "Full table scan" errors in daily query jobs | Warehouse optimizer can't prune when agent writes broad `WHERE` clauses |
| Agent attributes the wrong cause | No materialized change-point layer — agent has to infer "when" from raw data |
| Agent gives vague narratives | Nothing in the data layer tells it *what happened this week*; it has to rederive from counts |
| Inconsistent answers to the same question | Each agent run re-queries raw, gets slightly different samples, produces different conclusions |

### 2.3 What's been tried (and why it's not enough)
- **Dashboard pre-aggregates:** exist, but keyed by the dashboard dimensions the human analyst happened to need at build time. Agents ask different questions.
- **Feature stores for ML:** exist for production models (ranking, fraud), but are optimized for point-in-time lookups during inference, not for time-series reasoning.
- **Direct SQL access:** works for a senior analyst with context. For an agent it's a speed and cost disaster.

The missing piece is a data layer **shaped for reasoning over time**, not for serving dashboards or inference.

---

## 3. Target outcomes

| Dimension | Raw clickstream today | This layer (target) |
|---|---|---|
| p95 latency for a 12-month longitudinal query | 90s – 30min | **<2s** |
| Rows scanned per query | 10B – 100B | **<10k** (via pre-aggregation + change-point index) |
| Compute cost per query | $1 – $20 | **<$0.005** |
| Agent answer quality on longitudinal benchmark | ~40% (eval-graded) | **≥85%** |
| Max timeframe agents reliably handle | 4–6 weeks | **24 months** |
| Storage overhead vs. raw events | n/a | **<3%** |
| Time to ship a new derived metric | 1–2 sprints | **1 day** (declarative config) |

The storage number is load-bearing: the whole approach is only sellable if the AI-ready layer is small enough to fit comfortably alongside the warehouse, not replace it.

---

## 4. Non-goals

Explicitly *out of scope* for this PRD:
- **Replacing the warehouse.** Raw events stay where they are. This layer is a consumer of raw, not a replacement.
- **Real-time / streaming ingestion.** Batch-first. A hot path for ≤24h old metrics is a v2.
- **End-user dashboards.** This layer feeds agents. If analysts want to query it, fine, but no BI tooling is built here.
- **Training production ML models.** The embeddings here are for analytical reasoning, not for ranking or prediction.
- **Cross-tenant / cross-product data sharing.** Scoped to Google Play's loyalty & offers surface.

---

## 5. Users

| User | How they touch it | What they need |
|---|---|---|
| **Attribution agents** (primary) | Read layer 4 tables via a query API | Fast lookups, pre-decomposed trends, materialized change points |
| **Forecasting agents** | Read layer 3 (decomposed) | Clean seasonal/trend components to project forward |
| **Anomaly agents** | Read layer 4 change-point index | "What shifted and when?" in one query |
| **DS&A analysts** | Curate layer 4 definitions, validate narratives | Config-as-code, narrative QA tooling |
| **ML engineers** | Consume embeddings for downstream models | Stable schemas, versioned artifacts, lineage |
| **Workshop attendees** (prototype phase) | Watch the head-to-head demo | Visceral before/after |

---

## 6. Solution — the 5-layer pyramid

```
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 4 — AI-ready feature store                            │
 │  change_points · journey_archetypes · embeddings · narrative │
 │  ~100s of rows/week · <10ms lookup · agent-native            │
 └──────────────────────────────────────────────────────────────┘
                             ▲
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 3 — Decomposed weekly rollups                         │
 │  weekly(metric, dim) → trend + seasonal + residual (STL)     │
 │  ~10k rows/week · <100ms query                               │
 └──────────────────────────────────────────────────────────────┘
                             ▲
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 2 — Daily aggregates by dimension                     │
 │  daily × market × category × segment × metric                │
 │  ~100k rows/day · seconds to query                           │
 └──────────────────────────────────────────────────────────────┘
                             ▲
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 1 — Session aggregates                                │
 │  one row per user-session · sequence + duration + outcomes   │
 │  ~10M rows/day · minutes to query                            │
 └──────────────────────────────────────────────────────────────┘
                             ▲
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 0 — Raw events (exists in the warehouse already)      │
 │  ~100B events/day · petabyte-scale · hours to scan           │
 └──────────────────────────────────────────────────────────────┘
```

Each upward transition is **lossy by design**. The rule of every layer: *throw away what an agent doesn't need in order to reason about this timescale.*

### 6.1 Layer 0 — Raw events
- What's there: every click, impression, search, install, review, purchase.
- Source of truth. Kept in the warehouse, untouched by this project.
- Hot retention 30–90 days, archived beyond.
- **Agents should never query this directly.** That's the whole point.

### 6.2 Layer 1 — Session aggregates
- One row per user session: `user_id`, `session_id`, `started_at`, `duration_s`, `event_count`, `events_seq` (compact sequence of event types), `market`, `category`, `segment`, `had_install`, `had_purchase`, `had_offer_redeem`, `revenue_usd`.
- ~100× compression vs. raw.
- Built nightly, partitioned by date.

### 6.3 Layer 2 — Daily aggregates by dimension
- One row per `(date × market × category × segment)`.
- Pre-computed metrics: session count, unique users, events per session, install rate, purchase rate, offer redemption rate, revenue, D1/D7 retention proxies.
- ~1000× compression vs. Layer 1.
- Keyed exactly so agents can say "give me India Games casual users last week" without a join.

### 6.4 Layer 3 — Decomposed weekly rollups
- Weekly rollups of Layer 2 metrics.
- Each series runs through **STL decomposition** (seasonal + trend + residual) *at materialization time, not query time*.
- Result columns: `metric`, `dim_key`, `week_start`, `raw`, `trend`, `seasonal`, `residual`, `yoy_delta`, `wow_delta`, `trend_slope`, `residual_zscore`.
- **Critical insight:** the agent never decomposes series at query time. It reads pre-decomposed columns and reasons over `trend` vs. `residual`.
- This is what unlocks "is this movement structural or noise?" as a 1-row lookup.

### 6.5 Layer 4 — AI-ready feature store
Four materialized artifacts, all keyed for agent consumption:

1. **`change_points`** — every statistically significant shift in any (metric, dim_key) series, with date, direction, magnitude, pre/post means, and confidence. Built by running a change-point detector (CUSUM / Bayesian / `ruptures`) over Layer 3 weekly data on a schedule.
   *Agent query:* "Show me all change points for offer_redemption in India Games in the last 12 months." → 1 table scan, ~10 rows.
2. **`journey_archetypes`** — user sessions clustered into 50–100 archetypes (sequence-based clustering). Per (week × dim_key) the distribution is stored: `archetype_id`, `name`, `share_of_sessions`, `wow_delta`.
   *Agent query:* "Which archetypes are growing in India Games?" → 1 lookup.
3. **`behavioral_embeddings`** — per `(dim_key, week)`, a dense vector summarizing behavior along that week. Prototype uses PCA over a ~32-feature engineered vector. Production would use a sequence transformer on session traces.
   *Agent query:* "How similar is India Games behavior in Oct 2024 to Oct 2023?" → cosine similarity between two vectors.
4. **`narrative_event_log`** — per (week × dim_key), a short natural-language description of what happened, generated by a batch pipeline that reads Layers 2 + 3 + change points.
   *Agent query:* the agent reads the last N weekly narratives *as its context* and reasons over pre-digested prose instead of numeric tables.

The narrative log is the subtlest and arguably the most important. It turns months of history into ~100 short paragraphs, each of which the agent can hold in attention at once.

---

## 7. Five AI-native techniques (detail)

### 7.1 Tiered temporal summarization (baked-in decomposition)
- **Why:** agents' biggest mistake in longitudinal analysis is confusing seasonality for trend. Pre-decomposing means that mistake is impossible at the query layer.
- **How:** STL decomposition over weekly Layer-2 series. Period chosen per metric (52-week for annual, 4-week for monthly).
- **Prototype:** `statsmodels.tsa.seasonal.STL` if data is long enough; moving-average fallback otherwise.

### 7.2 Change-point detection as a materialized layer
- **Why:** "when did it start?" is the single most expensive class of longitudinal question. Running change-point detection at query time is a non-starter. Running it in batch over pre-decomposed series is cheap.
- **How:** CUSUM or Bayesian change-point over the `residual` column from Layer 3. Filter to points above a significance threshold, store with direction, magnitude, and 95% CI.
- **Prototype:** rolling Z-score threshold as a baseline, `ruptures.Pelt` as a richer option.

### 7.3 Journey archetype compression
- **Why:** "what kind of user behavior is changing?" can't be answered by scalar metrics. Clustering sessions into archetypes gives agents a vocabulary for talking about behavior.
- **How:** extract session-level features (duration, event mix, conversion indicator, sequence n-grams), cluster with KMeans (k=50), label clusters with descriptive names via the mode of their event sequences. Per-week archetype distribution is stored.
- **Prototype:** sklearn KMeans on 16-dim session-feature vectors; labels auto-generated from top event types per cluster.

### 7.4 Behavioral embeddings
- **Why:** scalar metrics can't capture "the shape of behavior changed." Embeddings can.
- **How:** per (dim_key × week), build a feature vector (archetype distribution + metric vector + trend slopes), then PCA to 8 dims for storage. Production version would swap PCA for a transformer over session sequences.
- **Prototype:** sklearn PCA. The payoff is the similarity query: cosine(week_a, week_b) tells you "how different does this week feel?"

### 7.5 Semantic event logs (the load-bearing innovation)
- **Why:** LLMs reason better over prose than over tables. A weekly narrative is literally the right shape for an LLM's context window.
- **How:** a batch pipeline reads Layers 2–4 for a given (week × dim_key), fills a deterministic template, and optionally rewrites with an LLM for polish. Key fact: the numeric values come from the pipeline — the LLM only rephrases. **No fact is ever generated by the LLM.**
- **Prototype:** template-based generator by default; Anthropic polish behind a flag.

---

## 8. Data contracts (target schemas)

### `layer2_daily`
```
date              DATE      partition key
market            STRING
category          STRING
segment           STRING
sessions          INT
unique_users      INT
events            INT
install_count     INT
purchase_count    INT
offer_impressions INT
offer_clicks      INT
offer_redeems     INT
revenue_usd       DOUBLE
d1_retention      DOUBLE
d7_retention      DOUBLE
```

### `layer3_decomposed`
```
week_start        DATE      partition key
metric            STRING    enum
dim_key           STRING    "market/category/segment"
raw               DOUBLE
trend             DOUBLE
seasonal          DOUBLE
residual          DOUBLE
trend_slope_4w    DOUBLE
residual_zscore   DOUBLE
wow_delta         DOUBLE
yoy_delta         DOUBLE    null if <52w history
```

### `layer4_change_points`
```
change_id         STRING    UUID
metric            STRING
dim_key           STRING
change_date       DATE
direction         STRING    "up" | "down"
magnitude_pct     DOUBLE
confidence        DOUBLE    0–1
pre_mean          DOUBLE
post_mean         DOUBLE
detection_method  STRING    "cusum" | "bayesian" | "ruptures_pelt"
```

### `layer4_journey_archetypes`
```
week_start        DATE
dim_key           STRING
archetype_id      INT
archetype_name    STRING
session_share     DOUBLE    0–1
wow_delta         DOUBLE
top_events        ARRAY<STRING>
```

### `layer4_embeddings`
```
week_start        DATE
dim_key           STRING
embedding         ARRAY<DOUBLE>  dim=8
feature_version   STRING
```

### `layer4_narrative_log`
```
week_start        DATE
dim_key           STRING
headline          STRING   one-line summary
body              STRING   ~200 words, agent-consumable
facts             JSON     structured evidence from the pipeline
generated_by      STRING   "template" | "template+llm-polish"
```

---

## 9. Example use case walkthrough

**Question:** *"Why did offer redemption rate drop 18% in India Games in late October 2024?"*

### Before this layer (today)
1. Agent writes `SELECT ... FROM raw_events WHERE event='offer_redeem' AND market='IN' AND ...`
2. Query scans 60 days × ~3B events/day = 180B rows.
3. Warehouse times out at 5 minutes or returns sampled results.
4. Agent retries with narrower window → loses the longitudinal view.
5. Final answer: "It dropped because fewer redemptions happened." (Tautology.)
6. Time: 8 minutes if it works. Cost: ~$12. Quality: 2/5.

### With this layer
1. Agent queries `layer4_change_points WHERE metric='offer_redeem_rate' AND dim_key LIKE 'IN/Games/%'`.
   → Returns 1 row: Δ -18% on 2024-10-29, confidence 0.96.
2. Agent queries `layer4_narrative_log WHERE week_start BETWEEN 2024-10-20 AND 2024-11-05 AND dim_key LIKE 'IN/Games/%'`.
   → Returns 3 short paragraphs. One says: *"Offer redemption fell sharply mid-week following the end of the Diwali promotion on Oct 28. Pre-Diwali baseline was 4.2%; post-Diwali 3.4%. The decline tracks the promotion calendar and is not a structural shift — Layer 3 trend slope is flat."*
3. Agent queries `layer4_journey_archetypes` to confirm archetype mix didn't shift.
   → Confirms.
4. Agent writes final answer with cited change point, narrative, and archetype stability.
5. Time: 450ms. Cost: $0.003. Quality: 4.5/5 (grounded in pre-computed evidence).

**The delta is not 10% better. It's 1000× faster and 1000× cheaper and materially more correct.** That's the pitch.

---

## 10. Success metrics

| Metric | Target | How measured |
|---|---|---|
| p95 latency on longitudinal benchmark (30 questions) | <2s | Instrumented query timer |
| Average rows scanned per query | <10k | DuckDB / engine stats |
| Agent answer quality (same LLM, same benchmark) | ≥85% vs. ≤40% raw-only | LLM-as-judge rubric from existing eval harness |
| Storage size vs. raw events | <3% | Parquet size comparison |
| Time to add a new derived metric | <1 day | Measured from dim add → Layer 4 availability |
| Narrative factual grounding rate | ≥99% | Random sample + programmatic fact-check vs. Layer 2 |

---

## 11. Build plan

### Phase 1 — Offline prototype (this sprint)
**Goal:** visceral demo for the workshop.
- Synthetic Play-style clickstream generator (1M+ events, 18 months, 5 markets × 5 categories × 4 segments)
- Full 5-layer pipeline, all 5 techniques implemented (PCA embeddings, template narratives, KMeans archetypes, rolling Z-score + `ruptures` change points, STL decomposition)
- Streamlit UI: architecture view + head-to-head query page
- Bootstrap script that builds everything end-to-end in one command
- **Deliverables in this repo:** `Longitudinal Data Setup/` with runnable pipeline and demo app

### Phase 2 — Incremental build pipeline (post-workshop, ~4 weeks)
- Airflow/Dagster DAG for nightly materialization
- Incremental Layer 1 → 2 → 3 (only rebuild affected partitions)
- Schema registry + data contracts
- Backfill tooling for config changes

### Phase 3 — Production feature store (~8 weeks)
- BigQuery-native implementation (since Google Play lives there)
- Agent SDK: Python client for `query_change_points()`, `query_narrative()`, `query_archetypes()`
- Integration into existing attribution agent pipeline
- A/B: same agent, half traffic on raw queries, half on this layer

### Phase 4 — Hot path (~12 weeks)
- Streaming layer for ≤24h old metrics via a merge-on-read pattern
- Real-time change-point alerting

---

## 12. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Narrative generation hallucinates facts | Medium | High | Template-first, LLM only rephrases; programmatic fact-check against Layer 2 |
| Archetypes drift as user behavior changes | High | Medium | Scheduled re-clustering (monthly); version the archetype schema |
| STL decomposition misfires on short history | Medium | Medium | Fall back to simple moving average for series <2 full seasons |
| Agents ignore Layer 4 and still query raw | Medium | High | Gate raw-event access behind a rate limit; promote Layer 4 in agent system prompts |
| Change-point false positives | Medium | Medium | Require minimum magnitude + confidence threshold; show "not a change point" explicitly |
| Embeddings uninterpretable | High | Low | Ship with reverse-lookup tool: given a vector, show top contributing features |
| Storage grows faster than linear in dimensions | Medium | Medium | Cap dimensional cardinality at materialization time; roll up long-tail segments |

---

## 13. Open decisions (need user validation)

1. **Scale of the prototype's synthetic data.** Default: 1,000 users × 18 months × ~3 events/user/day ≈ 1.6M raw events. Big enough to show a latency gap, small enough to regenerate in <60s. Alternatives: scale up to 5M+ for a more visceral demo, or down to 500k for faster iteration.
2. **Head-to-head demo fidelity.** Default: run both paths on local data for real, extrapolate production numbers, and make the LLM-in-the-loop optional. Alternative: always run real Anthropic calls — costs tokens but makes narrative quality visible.
3. **Whether to host this as a standalone Streamlit app or a page inside the main `play-attribution-intelligence` app.** Default: standalone, with a link card on the main landing page.
4. **Narrative generator — template only, or template + LLM polish?** Default: template-first with polish behind a flag. Alternative: always polish.
5. **Change-point detection algorithm.** Default: rolling Z-score for speed, `ruptures.Pelt` as the "hero" option. Alternative: Bayesian (bayesloop) — more expressive but heavier dependency.
6. **Embedding dimension.** Default: 8. Prototype PCA doesn't gain from more. Production transformer would use 64–256.
7. **Whether to ship the narrative log as the primary agent interface, or as an optional "context pack."** Default: optional. The agent can still read numeric tables; the narrative log is presented as a second, higher-signal channel.

Flagged for discussion at kickoff.

---

## 14. Appendix — prototype file layout

```
Longitudinal Data Setup/
├── PRD.md                              ← this file
├── README.md                           ← quick start + vision
├── ARCHITECTURE.md                     ← diagrams + contracts
├── requirements.txt
├── scripts/
│   └── bootstrap.sh                    ← generate + build + verify in one shot
├── src/
│   ├── config.py                       ← scale knobs, paths, initiative calendar
│   ├── generator/
│   │   ├── personas.py                 ← persona → event mix
│   │   ├── events.py                   ← Poisson-ish event simulator
│   │   └── run.py                      ← CLI entry
│   ├── pipeline/
│   │   ├── layer1_sessionize.py
│   │   ├── layer2_aggregate.py
│   │   ├── layer3_decompose.py         ← STL / moving-avg fallback
│   │   ├── change_points.py            ← rolling Z-score + ruptures option
│   │   ├── archetypes.py               ← KMeans on session features
│   │   ├── embeddings.py               ← PCA over engineered features
│   │   ├── narrative.py                ← template-first log generator
│   │   └── build.py                    ← orchestrator: 0 → 4 in one call
│   ├── query/
│   │   ├── naive_raw.py                ← pretend-we-have-no-layer baseline
│   │   └── ai_ready.py                 ← Layer 4 client
│   └── ui/
│       ├── app.py                      ← Streamlit entry
│       └── pages/
│           ├── 1_architecture.py       ← pyramid diagram + layer sizes
│           └── 2_head_to_head.py       ← the hero demo
└── data/                               ← gitignored; regenerated by bootstrap
    ├── raw/
    ├── layer1_sessions/
    ├── layer2_daily/
    ├── layer3_decomposed/
    └── layer4_ai_ready/
```

---

## 15. What "done" looks like for the prototype

A workshop attendee can:

1. Run `./scripts/bootstrap.sh` and see the pipeline build in one shot (<2 minutes).
2. Open the Streamlit app.
3. See the 5-layer architecture diagram with live row counts + storage sizes for their just-built data.
4. On the head-to-head page, type a longitudinal question, hit run, and watch:
   - **Left panel** — "naive" path scans raw events, displays real latency, rows read, and an extrapolated production cost in dollars.
   - **Right panel** — Layer 4 path returns in <100ms, shows which change points / archetypes / narratives it touched, and cites them.
5. Walk away convinced that the data layer, not the agent, is the lever.

That's the deliverable.
