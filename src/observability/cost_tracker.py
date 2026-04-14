"""Token and cost accounting for LLM API calls."""

from dataclasses import dataclass, field
from datetime import datetime

# Pricing per 1M tokens (USD) — approximate as of mid-2025
MODEL_PRICING = {
    "claude-sonnet-4-20250514": {"input": 3.0, "output": 15.0},
    "claude-haiku-4-5-20251001": {"input": 0.80, "output": 4.0},
    "claude-opus-4-6": {"input": 15.0, "output": 75.0},
    # Vertex Gemini stubs
    "gemini-1.5-pro": {"input": 3.50, "output": 10.50},
    "gemini-1.5-flash": {"input": 0.075, "output": 0.30},
}


@dataclass
class LLMCall:
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    timestamp: str
    stage: str = ""


@dataclass
class CostTracker:
    """Tracks token usage and costs across an agent run."""

    calls: list[LLMCall] = field(default_factory=list)
    current_stage: str = ""

    def record_call(self, model: str, input_tokens: int, output_tokens: int):
        pricing = MODEL_PRICING.get(model, {"input": 3.0, "output": 15.0})
        cost = (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000

        self.calls.append(LLMCall(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            timestamp=datetime.utcnow().isoformat(),
            stage=self.current_stage,
        ))

    def set_stage(self, stage: str):
        self.current_stage = stage

    @property
    def total_cost(self) -> float:
        return sum(c.cost_usd for c in self.calls)

    @property
    def total_input_tokens(self) -> int:
        return sum(c.input_tokens for c in self.calls)

    @property
    def total_output_tokens(self) -> int:
        return sum(c.output_tokens for c in self.calls)

    def summary(self) -> dict:
        return {
            "total_cost_usd": round(self.total_cost, 6),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "num_calls": len(self.calls),
            "calls": [
                {
                    "model": c.model,
                    "input_tokens": c.input_tokens,
                    "output_tokens": c.output_tokens,
                    "cost_usd": round(c.cost_usd, 6),
                    "stage": c.stage,
                }
                for c in self.calls
            ],
        }
