"""Prepare data for generation step."""

import logging
import polars as pl

from config import Config

logger = logging.getLogger(__name__)


def transform(df: pl.DataFrame, config: Config) -> pl.DataFrame:
    """Clean and prepare campaign data for batch processing."""
    # Strip whitespace from campaign names
    df = df.with_columns(pl.col("campaign_name").str.strip_chars())

    # Remove any empty rows
    df = df.filter(pl.col("campaign_name").str.len_chars() > 0)

    logger.info(f"Prepared {len(df)} campaigns for processing")
    return df
