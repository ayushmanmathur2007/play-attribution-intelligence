"""Orchestrator: run the full 6-stage attribution reasoning pipeline."""

import json
import logging
import yaml
from pathlib import Path

from ..observability.tracer import Tracer
from ..observability.cost_tracker import CostTracker
from .llm_client import LLMClientFactory
from .data_client import DataClientFactory
from .query_parser import QueryParser
from .data_fetcher import DataFetcher
from .context_enricher import ContextEnricher
from .attribution_reasoner import AttributionReasoner
from .grounding_checker import GroundingChecker
from .narrative_generator import NarrativeGenerator

logger = logging.getLogger(__name__)


class AttributionPipeline:
    """End-to-end attribution pipeline.

    Stages:
        1. QueryParser       — NL query -> structured params
        2. DataFetcher        — structured params -> data slices
        3. ContextEnricher    — data + domain knowledge -> enriched context
        4. AttributionReasoner — enriched context -> structured attribution
        5. GroundingChecker   — attribution + data -> verified claims
        6. NarrativeGenerator — verified attribution -> markdown report
    """

    def __init__(self, config_path: str = "config/local.yaml"):
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(
                f"Config file not found: {config_path}. "
                f"Expected at {config_file.resolve()}"
            )

        with open(config_file) as f:
            self.config = yaml.safe_load(f)

        # Observability
        self.cost_tracker = CostTracker()
        trace_output = (
            self.config.get("observability", {}).get("trace_output", "traces")
        )
        self.tracer = Tracer(output_dir=trace_output)

        # Clients
        self.llm = LLMClientFactory.create(
            self.config.get("llm", {}), self.cost_tracker
        )
        self.data_client = DataClientFactory.create(self.config.get("data", {}))

        # Load dimensions for query parser
        dimensions_path = Path("config/dimensions.yaml")
        if dimensions_path.exists():
            with open(dimensions_path) as f:
                self.dimensions = yaml.safe_load(f)
        else:
            logger.warning(
                "dimensions.yaml not found at %s, query parser will have "
                "limited dimension awareness",
                dimensions_path.resolve(),
            )
            self.dimensions = {}

        # Stages
        self.parser = QueryParser(self.llm, self.dimensions)
        self.fetcher = DataFetcher(self.data_client)
        self.enricher = ContextEnricher(
            self.config.get("data", {}).get("data_dir", "data/synthetic")
        )
        self.reasoner = AttributionReasoner(self.llm)
        self.checker = GroundingChecker(self.llm)
        self.narrator = NarrativeGenerator(self.llm)

    async def process(self, query: str) -> dict:
        """Run the full 6-stage pipeline on a natural language query.

        Returns dict with keys:
            parsed_query, attribution, grounding, narrative, trace, cost.
        On error, returns dict with: error, trace, cost.
        """
        trace = self.tracer.start_trace(query)

        try:
            # Stage 1: Parse query
            self.cost_tracker.set_stage("query_parser")
            self.tracer.start_stage("query_parser", query)
            logger.info("Stage 1/6: Parsing query")
            parsed = await self.parser.parse(query)
            self.tracer.end_stage(
                json.dumps(parsed, default=str)[:200]
            )
            logger.info(
                "Parsed: metric=%s, market=%s, period=%s",
                parsed.get("metric"),
                parsed.get("market"),
                parsed.get("period", {}).get("description"),
            )

            # Stage 2: Fetch data
            self.cost_tracker.set_stage("data_fetcher")
            self.tracer.start_stage(
                "data_fetcher",
                json.dumps(parsed, default=str)[:200],
            )
            logger.info("Stage 2/6: Fetching data")
            fetched = self.fetcher.fetch(parsed)
            fetch_summary = ", ".join(
                f"{k}: {len(v) if hasattr(v, '__len__') else '?'} rows"
                for k, v in fetched.items()
            )
            self.tracer.end_stage(f"Fetched {len(fetched)} sections: {fetch_summary[:300]}")
            logger.info("Fetched data sections: %s", fetch_summary)

            # Stage 3: Enrich context
            self.cost_tracker.set_stage("context_enricher")
            self.tracer.start_stage("context_enricher", "Enriching with domain knowledge")
            logger.info("Stage 3/6: Enriching context")
            enriched = self.enricher.enrich(parsed, fetched)
            self.tracer.end_stage("Context enriched")

            # Stage 4: Attribution reasoning (LLM)
            self.cost_tracker.set_stage("attribution_reasoner")
            self.tracer.start_stage(
                "attribution_reasoner",
                f"Reasoning about {parsed.get('metric')} in {parsed.get('market')}",
            )
            logger.info("Stage 4/6: Attribution reasoning (LLM call)")
            attribution = await self.reasoner.reason(enriched)
            attr_summary = (
                f"{len(attribution.get('attribution', []))} causes, "
                f"confidence={attribution.get('overall_confidence')}"
            )
            self.tracer.end_stage(attr_summary)
            logger.info("Attribution: %s", attr_summary)

            # Stage 5: Grounding check
            self.cost_tracker.set_stage("grounding_checker")
            self.tracer.start_stage(
                "grounding_checker",
                f"Verifying {len(attribution.get('attribution', []))} claims",
            )
            logger.info("Stage 5/6: Grounding check (LLM call)")
            grounded = await self.checker.check(attribution, fetched)
            grounding_score = grounded.get("grounding_score", "N/A")
            self.tracer.end_stage(f"Grounding score: {grounding_score}")
            logger.info("Grounding score: %s", grounding_score)

            # Stage 6: Narrative generation (LLM)
            self.cost_tracker.set_stage("narrative_generator")
            self.tracer.start_stage(
                "narrative_generator",
                "Generating executive narrative",
            )
            logger.info("Stage 6/6: Narrative generation (LLM call)")
            narrative = await self.narrator.generate(grounded, parsed, enriched)
            self.tracer.end_stage(narrative[:200])
            logger.info("Narrative generated (%d chars)", len(narrative))

            trace_result = self.tracer.end_trace(self.cost_tracker.summary())

            return {
                "parsed_query": parsed,
                "attribution": attribution,
                "grounding": grounded,
                "narrative": narrative,
                "trace": trace_result,
                "cost": self.cost_tracker.summary(),
            }

        except Exception as e:
            logger.exception("Pipeline failed: %s", e)
            # Ensure we close out any open stage
            self.tracer.log_error(str(e))
            self.tracer.end_stage(f"Error: {e}")
            trace_result = self.tracer.end_trace(self.cost_tracker.summary())
            return {
                "error": str(e),
                "trace": trace_result,
                "cost": self.cost_tracker.summary(),
            }

    def close(self):
        """Release resources held by the pipeline."""
        try:
            self.data_client.close()
        except Exception as e:
            logger.debug("Error closing data client: %s", e)
