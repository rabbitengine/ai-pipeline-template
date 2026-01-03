"""Extract campaign data from BigQuery bronze table."""

import logging
import polars as pl
from google.cloud import bigquery
import google.auth

from config import Config

logger = logging.getLogger(__name__)


def extract(config: Config) -> pl.DataFrame:
    """Read campaign names from BigQuery bronze table."""
    credentials, project = google.auth.default()
    client = bigquery.Client(
        credentials=credentials,
        project=config.source.project or project
    )

    table_id = f"{client.project}.{config.source.dataset}.{config.source.table}"

    query = f"SELECT * FROM `{table_id}`"
    result = client.query(query).result()

    # Convert to Polars DataFrame
    df = pl.from_arrow(result.to_arrow())

    logger.info(f"Extracted {len(df)} campaigns from {table_id}")
    return df
