"""Orchestrate the AI pipeline: extract → transform → generate → load."""

import logging

from config import load_config
from extract import extract
from transform import transform
from generate import generate
from load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run() -> None:
    """Run the complete AI pipeline."""
    logger.info("Starting AI pipeline...")

    config = load_config()

    # Extract from BigQuery bronze
    df = extract(config)

    # Transform: prepare prompts
    df = transform(df, config)

    # Generate: call Claude API
    df = generate(df, config)

    # Load to BigQuery silver
    result = load(df, config)

    logger.info(f"Pipeline complete! Loaded {result['rows_loaded']} rows to {result['table_id']}")


if __name__ == "__main__":
    run()
