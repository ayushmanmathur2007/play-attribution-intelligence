"""Build the entire 5-layer pipeline end-to-end.

Usage:
    python -m src.pipeline.build           # run everything
    python -m src.pipeline.build --stages layer2,layer3
    python -m src.pipeline.build --from layer3    # skip earlier stages

Each stage prints its own timing line; this script prints a summary at the end.
"""

from __future__ import annotations

import argparse
import time
from typing import Callable

from . import (
    archetypes,
    causal_candidates,
    change_points,
    embeddings,
    layer1_sessionize,
    layer2_aggregate,
    layer3_decompose,
    narrative,
)

# Ordered list of stages. Each entry is (stage_id, run_fn).
STAGES: list[tuple[str, Callable[[], object]]] = [
    ("layer1_sessionize", layer1_sessionize.run),
    ("layer2_aggregate", layer2_aggregate.run),
    ("layer3_decompose", layer3_decompose.run),
    ("change_points", change_points.run),
    ("archetypes", archetypes.run),
    ("narrative", narrative.run),
    ("embeddings", embeddings.run),
    ("causal_candidates", causal_candidates.run),
]
STAGE_IDS = [s[0] for s in STAGES]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--stages",
        type=str,
        default=None,
        help="Comma-separated list of stage ids to run (default: all).",
    )
    p.add_argument(
        "--from",
        dest="from_stage",
        type=str,
        default=None,
        help="Run this stage and everything after it.",
    )
    return p.parse_args()


def _select_stages(args: argparse.Namespace) -> list[tuple[str, Callable]]:
    if args.stages:
        wanted = [s.strip() for s in args.stages.split(",") if s.strip()]
        unknown = [s for s in wanted if s not in STAGE_IDS]
        if unknown:
            raise SystemExit(f"Unknown stage(s): {unknown}. Valid: {STAGE_IDS}")
        return [s for s in STAGES if s[0] in wanted]
    if args.from_stage:
        if args.from_stage not in STAGE_IDS:
            raise SystemExit(f"Unknown stage: {args.from_stage}. Valid: {STAGE_IDS}")
        start_idx = STAGE_IDS.index(args.from_stage)
        return STAGES[start_idx:]
    return STAGES


def main() -> None:
    args = _parse_args()
    selected = _select_stages(args)

    print(f"[build] running {len(selected)} stage(s): {[s[0] for s in selected]}")
    overall_t0 = time.time()
    timings: list[tuple[str, float]] = []
    for stage_id, fn in selected:
        print(f"[build] → {stage_id}")
        t0 = time.time()
        fn()
        timings.append((stage_id, time.time() - t0))

    total = time.time() - overall_t0
    print("\n[build] summary:")
    for sid, secs in timings:
        print(f"  {sid:<22} {secs:>7.2f}s")
    print(f"  {'TOTAL':<22} {total:>7.2f}s")


if __name__ == "__main__":
    main()
