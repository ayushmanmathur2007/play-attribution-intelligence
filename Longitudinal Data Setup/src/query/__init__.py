"""Two query paths, side-by-side, for the same longitudinal questions.

- `naive_raw`  : the "before" — scan billions of raw events to answer.
- `ai_ready`   : the "after"  — read pre-digested layer-4 artifacts.

Both modules expose a `QueryResult` dataclass so the UI can compare them
cell-by-cell: latency, bytes scanned (local + extrapolated to Play scale),
cost estimate, and the actual answer text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryResult:
    path: str  # "naive_raw" | "ai_ready"
    question: str
    answer: str
    facts: list[dict] = field(default_factory=list)
    wall_time_s: float = 0.0
    rows_scanned_local: int = 0
    bytes_scanned_local: int = 0
    rows_scanned_extrapolated: int = 0
    bytes_scanned_extrapolated: int = 0
    extras: dict[str, Any] = field(default_factory=dict)

    def as_row(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "wall_time_s": round(self.wall_time_s, 3),
            "rows_local": self.rows_scanned_local,
            "bytes_local": self.bytes_scanned_local,
            "rows_extrapolated": self.rows_scanned_extrapolated,
            "bytes_extrapolated": self.bytes_scanned_extrapolated,
        }
