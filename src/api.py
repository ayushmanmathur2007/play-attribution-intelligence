"""FastAPI server for the Play Attribution Intelligence system."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Play Attribution Intelligence",
    description="AI-powered metric attribution system for Google Play Loyalty & Offers",
    version="1.0.0",
)


class QueryRequest(BaseModel):
    query: str
    config_path: str = "config/local.yaml"


class QueryResponse(BaseModel):
    narrative: str
    attribution: dict
    grounding: dict
    parsed_query: dict
    cost: dict
    trace: dict


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/analyze", response_model=QueryResponse)
async def analyze(request: QueryRequest):
    try:
        from src.agent.pipeline import AttributionPipeline

        pipeline = AttributionPipeline(config_path=request.config_path)
        result = await pipeline.process(request.query)

        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])

        return QueryResponse(
            narrative=result.get("narrative", ""),
            attribution=result.get("attribution", {}),
            grounding=result.get("grounding", {}),
            parsed_query=result.get("parsed_query", {}),
            cost=result.get("cost", {}),
            trace=result.get("trace", {}),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/examples")
async def examples():
    return {
        "queries": [
            "Why did offer redemption rate increase 22% in India in late October 2024?",
            "What caused Play Points burn rate to spike 45% in India in late October?",
            "Why did DAU drop 15% globally on November 8-9 2024?",
            "What drove offer driven revenue decline in US Casual Games in early November?",
        ]
    }
