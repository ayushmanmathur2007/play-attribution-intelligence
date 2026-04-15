"""Layer 4: behavioral embeddings — one vector per (week_start x dim_key).

Two paths, produced side-by-side so the demo can show the contrast:

1. PCA baseline (the "before" contrast)
   - Start from the layer3 weekly decomposed table.
   - Pivot wide so each (week, dim_key) row has all decomposable metrics as columns.
   - StandardScale, then PCA to EMBEDDING_DIM.
   - Cheap, mechanical, context-free — you can tell two weeks apart but you
     can't describe *why* they're different.

2. Semantic embedding (the hero)
   - Load the layer4 narrative log.
   - Embed `headline + body` with sentence-transformers/all-MiniLM-L6-v2
     (384-dim, runs locally on CPU, no API key).
   - These encode natural-language meaning — "Diwali surge" and "Holi surge"
     cluster even though the numeric features are quite different, because
     the narrative text uses shared vocabulary.
   - We store both the full 384-dim vector AND an EMBEDDING_DIM PCA
     projection for symmetry with the baseline.

Output: data/layer4_ai_ready/embeddings.parquet
    columns:
      week_start, dim_key,
      embedding_pca        (list[float], len = EMBEDDING_DIM)
      embedding_semantic   (list[float], len = 384)
      embedding_semantic_pca (list[float], len = EMBEDDING_DIM)
      method_pca, method_semantic
"""

from __future__ import annotations

import time
import warnings

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from .. import config

_SEMANTIC_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


# ---------------------------------------------------------------------------
# PCA baseline
# ---------------------------------------------------------------------------


def _pca_embeddings(weekly: pd.DataFrame) -> pd.DataFrame:
    """Build PCA embeddings from the layer3 decomposed table."""
    # Pivot to wide: rows = (week, dim), cols = metric x {raw, wow_delta, trend_slope_4w}
    value_cols = ["raw", "wow_delta", "trend_slope_4w", "residual_zscore"]
    wide = weekly.pivot_table(
        index=["week_start", "dim_key"],
        columns="metric",
        values=value_cols,
        aggfunc="first",
    )
    wide.columns = [f"{a}__{b}" for a, b in wide.columns]
    wide = wide.fillna(0.0).reset_index()

    feature_cols = [c for c in wide.columns if c not in ("week_start", "dim_key")]
    X = wide[feature_cols].to_numpy(dtype=float)
    if X.shape[0] == 0:
        return pd.DataFrame(columns=["week_start", "dim_key", "embedding_pca"])

    # pct_change can produce ±inf when the prior value is 0 — clip them out
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    X = StandardScaler().fit_transform(X)
    k = min(config.EMBEDDING_DIM, X.shape[1], max(1, X.shape[0] - 1))
    pca = PCA(n_components=k, random_state=config.SEED)
    Z = pca.fit_transform(X)

    # Pad to EMBEDDING_DIM if we got fewer components (edge case: tiny data)
    if Z.shape[1] < config.EMBEDDING_DIM:
        pad = np.zeros((Z.shape[0], config.EMBEDDING_DIM - Z.shape[1]))
        Z = np.hstack([Z, pad])

    out = wide[["week_start", "dim_key"]].copy()
    out["embedding_pca"] = list(Z.astype(np.float32))
    return out


# ---------------------------------------------------------------------------
# Semantic embedding — sentence-transformers
# ---------------------------------------------------------------------------


def _semantic_embeddings(narratives: pd.DataFrame) -> pd.DataFrame:
    """Embed narrative text with a local sentence-transformer."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print(
            "  [layer4/embed] sentence-transformers not installed — "
            "skipping semantic path. `pip install sentence-transformers`"
        )
        return pd.DataFrame(
            columns=[
                "week_start",
                "dim_key",
                "embedding_semantic",
                "embedding_semantic_pca",
            ]
        )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = SentenceTransformer(_SEMANTIC_MODEL_NAME)

    texts = (narratives["headline"].astype(str) + ". " + narratives["body"].astype(str)).tolist()
    vecs = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=True,
    ).astype(np.float32)

    # Also project to EMBEDDING_DIM for storage symmetry with the baseline
    if vecs.shape[0] >= 2:
        k = min(config.EMBEDDING_DIM, vecs.shape[1], vecs.shape[0] - 1)
        pca_proj = PCA(n_components=k, random_state=config.SEED).fit_transform(vecs)
        if pca_proj.shape[1] < config.EMBEDDING_DIM:
            pad = np.zeros((pca_proj.shape[0], config.EMBEDDING_DIM - pca_proj.shape[1]))
            pca_proj = np.hstack([pca_proj, pad])
    else:
        pca_proj = np.zeros((vecs.shape[0], config.EMBEDDING_DIM), dtype=np.float32)

    out = narratives[["week_start", "dim_key"]].copy()
    out["embedding_semantic"] = list(vecs)
    out["embedding_semantic_pca"] = list(pca_proj.astype(np.float32))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> pd.DataFrame:
    t0 = time.time()

    weekly = pd.read_parquet(config.LAYER3_DIR / "weekly_decomposed.parquet")
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    pca_df = _pca_embeddings(weekly)

    narr_path = config.LAYER4_DIR / "narrative_log.parquet"
    if narr_path.exists():
        narratives = pd.read_parquet(narr_path)
        narratives["week_start"] = pd.to_datetime(narratives["week_start"])
        semantic_df = _semantic_embeddings(narratives)
    else:
        print("  [layer4/embed] narrative_log.parquet missing — semantic path skipped")
        semantic_df = pd.DataFrame(
            columns=[
                "week_start",
                "dim_key",
                "embedding_semantic",
                "embedding_semantic_pca",
            ]
        )

    merged = pca_df.merge(semantic_df, on=["week_start", "dim_key"], how="outer")
    merged["method_pca"] = "pca_on_layer3_wide"
    merged["method_semantic"] = (
        _SEMANTIC_MODEL_NAME if "embedding_semantic" in merged.columns else "none"
    )

    out_path = config.LAYER4_DIR / "embeddings.parquet"
    merged.to_parquet(out_path, index=False, compression="snappy")

    elapsed = time.time() - t0
    have_semantic = (
        "embedding_semantic" in merged.columns
        and merged["embedding_semantic"].notna().any()
    )
    print(
        f"  [layer4/embed] {len(merged):,} embeddings "
        f"(pca={config.EMBEDDING_DIM}d"
        f"{', semantic=384d' if have_semantic else ''}) "
        f"-> {out_path.name} in {elapsed:.1f}s"
    )
    return merged


if __name__ == "__main__":
    run()
