"""Stage 6: Generate analyst-quality narrative report from verified attribution."""

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class NarrativeGenerator:
    """Transform verified attribution data into an executive-ready
    markdown narrative report.
    """

    def __init__(self, llm_client):
        self.llm = llm_client
        self.prompt_template = (
            Path(__file__).parent / "prompts" / "narrative_generator.txt"
        ).read_text()

    async def generate(
        self,
        verified_attribution: dict,
        parsed_query: dict,
        enriched_context: dict,
    ) -> str:
        """Generate analyst-quality narrative report.

        Args:
            verified_attribution: Output from GroundingChecker.check()
            parsed_query: Output from QueryParser.parse()
            enriched_context: Output from ContextEnricher.enrich()

        Returns:
            Markdown-formatted narrative string.
        """
        system_prompt = self._build_prompt(
            verified_attribution, parsed_query, enriched_context
        )
        user_prompt = (
            f"Write an executive attribution report for the movement of "
            f"{parsed_query.get('metric', 'the metric')} in "
            f"{parsed_query.get('market', 'ALL')} market."
        )

        response = await self.llm.complete(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.3,
            max_tokens=2048,
            response_format=None,  # Free text, not JSON
        )

        narrative = self._post_process(response)
        return narrative

    def _build_prompt(
        self,
        verified_attribution: dict,
        parsed_query: dict,
        enriched_context: dict,
    ) -> str:
        """Fill the narrative prompt template with all context."""
        # Build the verified attribution JSON for the prompt
        verified_json = json.dumps(
            {
                "movement_confirmed": verified_attribution.get("movement_confirmed"),
                "magnitude": verified_attribution.get("magnitude"),
                "attribution": verified_attribution.get("attribution", []),
                "ruled_out": verified_attribution.get("ruled_out", []),
                "data_quality_flags": verified_attribution.get("data_quality_flags", []),
                "overall_confidence": verified_attribution.get("overall_confidence"),
                "verified_claims": verified_attribution.get("verified_claims", []),
                "grounding_score": verified_attribution.get("grounding_score"),
                "critical_issues": verified_attribution.get("critical_issues", []),
                "recommendations": verified_attribution.get("recommendations", []),
            },
            indent=2,
            default=str,
        )

        # Build magnitude description
        magnitude = verified_attribution.get("magnitude", "unknown")
        if isinstance(magnitude, (int, float)):
            magnitude = f"{magnitude}%"

        period = parsed_query.get("period", {})
        period_str = (
            f"{period.get('description', '')} "
            f"({period.get('start_date', '?')} to {period.get('end_date', '?')})"
        ).strip()

        result = self.prompt_template
        for placeholder, value in {
            "{verified_attribution}": verified_json,
            "{metric_name}": parsed_query.get("metric", "unknown_metric"),
            "{market}": parsed_query.get("market", "ALL"),
            "{period}": period_str,
            "{magnitude}": str(magnitude),
            "{metric_definition}": enriched_context.get(
                "metric_definition", "No definition available."
            ),
        }.items():
            result = result.replace(placeholder, str(value))
        return result

    def _post_process(self, response: str) -> str:
        """Clean up the LLM narrative output."""
        text = response.strip()

        # Remove any leading/trailing markdown code fences (shouldn't happen
        # with free text mode, but just in case)
        if text.startswith("```"):
            text = re.sub(r"^```(?:markdown)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        # Ensure the report has the expected section headers
        # If the LLM skipped a section, that's fine — we don't force structure
        # beyond what the prompt requests.

        # Trim excessive whitespace between sections
        text = re.sub(r"\n{4,}", "\n\n\n", text)

        return text
