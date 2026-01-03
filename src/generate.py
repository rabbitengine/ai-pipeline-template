"""Call Claude API to standardize campaign names."""

import logging
import json
import polars as pl
from anthropic import Anthropic

from config import Config

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # Campaigns per API call
MAX_TOKENS = 8192  # Enough for ~250 results

BATCH_PROMPT = """Parse these campaign names and extract structured fields for each.

Campaign names:
{campaigns}

For each campaign, extract:
- source: The ad platform (facebook, google, tiktok, linkedin, etc.)
- objective: The campaign goal (prospecting, retargeting, brand, conversion, awareness, etc.)
- geo: The target geography as 2-letter code (US, UK, DE, FR, etc.)
- audience: The audience type (cold, warm, lookalike, etc.) or null if not specified

Return a JSON array with one object per campaign, in the same order:
[{{"campaign_name": "...", "source": "...", "objective": "...", "geo": "...", "audience": "..."}}]

Return JSON only, no explanation."""


def _parse_response(text: str) -> list[dict]:
    """Parse Claude response, handling markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
        text = text.strip()
    return json.loads(text)


def _process_batch(client: Anthropic, campaigns: list[str], model: str) -> list[dict]:
    """Process a single batch of campaigns."""
    campaigns_text = "\n".join(f"- {name}" for name in campaigns)

    response = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        messages=[{"role": "user", "content": BATCH_PROMPT.format(campaigns=campaigns_text)}]
    )

    try:
        return _parse_response(response.content[0].text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse batch: {e}")
        return [
            {"campaign_name": name, "source": None, "objective": None, "geo": None, "audience": None}
            for name in campaigns
        ]


def generate(df: pl.DataFrame, config: Config) -> pl.DataFrame:
    """Call Claude API with automatic chunking for large datasets."""
    client = Anthropic()
    campaigns = df["campaign_name"].to_list()

    # Chunk into batches
    batches = [campaigns[i:i + BATCH_SIZE] for i in range(0, len(campaigns), BATCH_SIZE)]

    if len(batches) == 1:
        logger.info(f"Sending {len(campaigns)} campaigns to Claude...")
    else:
        logger.info(f"Sending {len(campaigns)} campaigns in {len(batches)} batches...")

    all_results = []
    for i, batch in enumerate(batches):
        if len(batches) > 1:
            logger.info(f"Processing batch {i + 1}/{len(batches)} ({len(batch)} campaigns)...")

        results = _process_batch(client, batch, config.model.model_name)
        all_results.extend(results)

    logger.info(f"Parsed {len(all_results)} campaigns successfully")

    # Join parsed fields back to original data (preserves metrics)
    parsed_df = pl.DataFrame(all_results)
    result = df.join(
        parsed_df.select(["campaign_name", "source", "objective", "geo", "audience"]),
        on="campaign_name",
        how="left"
    )
    return result
