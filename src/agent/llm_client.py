"""LLM abstraction layer — swap between Anthropic (local) and Vertex Gemini (GCP)."""

from abc import ABC, abstractmethod
import asyncio
import json
import logging
import time
import anthropic
from ..observability.cost_tracker import CostTracker

logger = logging.getLogger(__name__)


class LLMClient(ABC):
    @abstractmethod
    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        response_format: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def get_model_name(self) -> str:
        pass


class AnthropicClient(LLMClient):
    """Local development — uses Claude API."""

    def __init__(self, model: str = "claude-sonnet-4-20250514", cost_tracker: CostTracker | None = None):
        self.client = anthropic.Anthropic()
        self.model = model
        self.cost_tracker = cost_tracker

    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        response_format: str | None = None,
    ) -> str:
        prompt_suffix = ""
        if response_format == "json":
            prompt_suffix = "\n\nRespond ONLY with valid JSON. No markdown fences, no commentary."

        max_retries = 5
        base_delay = 30  # seconds — generous for 30K tokens/min limit

        for attempt in range(max_retries):
            try:
                message = self.client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": user_prompt + prompt_suffix}
                    ],
                )

                if self.cost_tracker:
                    self.cost_tracker.record_call(
                        model=self.model,
                        input_tokens=message.usage.input_tokens,
                        output_tokens=message.usage.output_tokens,
                    )

                return message.content[0].text

            except anthropic.RateLimitError as e:
                if attempt == max_retries - 1:
                    raise
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "Rate limited (attempt %d/%d), waiting %ds before retry...",
                    attempt + 1, max_retries, delay,
                )
                await asyncio.sleep(delay)

            except anthropic.APIStatusError as e:
                if e.status_code == 529 and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        "API overloaded (attempt %d/%d), waiting %ds...",
                        attempt + 1, max_retries, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

    def get_model_name(self) -> str:
        return self.model


class VertexGeminiClient(LLMClient):
    """GCP deployment — uses Vertex AI Gemini API. Stub for portability."""

    def __init__(self, model: str = "gemini-1.5-pro", project_id: str = "", location: str = "us-central1"):
        self.model = model
        self.project_id = project_id
        self.location = location

    async def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 4096,
        response_format: str | None = None,
    ) -> str:
        raise NotImplementedError(
            "Vertex Gemini client is a stub for GCP portability. "
            "Install google-cloud-aiplatform and implement the API call."
        )

    def get_model_name(self) -> str:
        return self.model


class LLMClientFactory:
    @staticmethod
    def create(config: dict, cost_tracker: CostTracker | None = None) -> LLMClient:
        provider = config.get("provider", "anthropic")
        if provider == "anthropic":
            return AnthropicClient(
                model=config.get("model", "claude-sonnet-4-20250514"),
                cost_tracker=cost_tracker,
            )
        elif provider == "vertex_gemini":
            return VertexGeminiClient(
                model=config.get("model", "gemini-1.5-pro"),
                project_id=config.get("project_id", ""),
                location=config.get("location", "us-central1"),
            )
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")

    @staticmethod
    def create_eval_client(config: dict, cost_tracker: CostTracker | None = None) -> LLMClient:
        """Create a client using the eval model (cheaper, for LLM-as-judge)."""
        eval_config = {**config, "model": config.get("eval_model", "claude-haiku-4-5-20251001")}
        return LLMClientFactory.create(eval_config, cost_tracker)
