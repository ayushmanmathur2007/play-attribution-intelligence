"""JSON file exporter for traces — local development."""

import json
from pathlib import Path
from datetime import datetime


class JSONExporter:
    """Export traces and metrics to local JSON files."""

    def __init__(self, output_dir: str = "traces"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_trace(self, trace: dict):
        trace_id = trace.get("trace_id", "unknown")[:8]
        filepath = self.output_dir / f"trace_{trace_id}.json"
        with open(filepath, "w") as f:
            json.dump(trace, f, indent=2, default=str)
        return str(filepath)

    def export_metrics(self, metrics: dict):
        filepath = self.output_dir / f"metrics_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filepath, "w") as f:
            json.dump(metrics, f, indent=2, default=str)
        return str(filepath)
