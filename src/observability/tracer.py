"""Execution tracing for agent pipeline runs."""

import json
import time
import uuid
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict


@dataclass
class StageTrace:
    stage_name: str
    start_time: float = 0.0
    end_time: float = 0.0
    duration_ms: float = 0.0
    input_summary: str = ""
    output_summary: str = ""
    llm_calls: list[dict] = field(default_factory=list)
    sql_queries: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class Trace:
    trace_id: str
    query: str
    timestamp: str
    stages: list[StageTrace] = field(default_factory=list)
    total_duration_ms: float = 0.0
    total_cost_usd: float = 0.0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    eval_scores: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


class Tracer:
    """JSON file-based tracer for local development. Swap for Cloud Trace on GCP."""

    def __init__(self, output_dir: str = "traces"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._current_trace: Trace | None = None
        self._current_stage: StageTrace | None = None
        self._trace_start: float = 0.0

    def start_trace(self, query: str) -> Trace:
        self._trace_start = time.time()
        self._current_trace = Trace(
            trace_id=str(uuid.uuid4()),
            query=query,
            timestamp=datetime.utcnow().isoformat(),
        )
        return self._current_trace

    def start_stage(self, stage_name: str, input_summary: str = ""):
        stage = StageTrace(
            stage_name=stage_name,
            start_time=time.time(),
            input_summary=input_summary[:500],
        )
        self._current_stage = stage

    def log_sql(self, sql: str):
        if self._current_stage:
            self._current_stage.sql_queries.append(sql)

    def log_llm_call(self, model: str, input_tokens: int, output_tokens: int, cost_usd: float, latency_ms: float):
        if self._current_stage:
            self._current_stage.llm_calls.append({
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_usd": cost_usd,
                "latency_ms": latency_ms,
            })

    def log_error(self, error: str):
        if self._current_stage:
            self._current_stage.errors.append(error)

    def end_stage(self, output_summary: str = ""):
        if self._current_stage:
            self._current_stage.end_time = time.time()
            self._current_stage.duration_ms = (
                (self._current_stage.end_time - self._current_stage.start_time) * 1000
            )
            self._current_stage.output_summary = output_summary[:500]
            if self._current_trace:
                self._current_trace.stages.append(self._current_stage)
            self._current_stage = None

    def end_trace(self, cost_summary: dict | None = None) -> dict:
        if not self._current_trace:
            return {}

        self._current_trace.total_duration_ms = (time.time() - self._trace_start) * 1000

        if cost_summary:
            self._current_trace.total_cost_usd = cost_summary.get("total_cost_usd", 0)
            self._current_trace.total_input_tokens = cost_summary.get("total_input_tokens", 0)
            self._current_trace.total_output_tokens = cost_summary.get("total_output_tokens", 0)

        trace_dict = self._current_trace.to_dict()

        # Write to JSON file
        filepath = self.output_dir / f"trace_{self._current_trace.trace_id[:8]}.json"
        with open(filepath, "w") as f:
            json.dump(trace_dict, f, indent=2, default=str)

        self._current_trace = None
        return trace_dict
