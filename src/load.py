"""Load standardized campaigns to BigQuery silver table."""

import logging
import io
import polars as pl
from google.cloud import bigquery
import google.auth

from config import Config

logger = logging.getLogger(__name__)


def load(df: pl.DataFrame, config: Config) -> dict:
    """Load DataFrame to BigQuery silver table."""
    credentials, project = google.auth.default()
    client = bigquery.Client(
        credentials=credentials,
        project=config.destination.project or project
    )

    # Create dataset if not exists
    dataset_ref = client.dataset(config.destination.dataset)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset: {config.destination.dataset}")

    table_id = f"{client.project}.{config.destination.dataset}.{config.destination.table}"

    # Write to BigQuery via Parquet (no pandas)
    with io.BytesIO() as stream:
        df.write_parquet(stream)
        stream.seek(0)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE"
        )

        job = client.load_table_from_file(stream, table_id, job_config=job_config)
        job.result()

    logger.info(f"Loaded {len(df)} rows to {table_id}")

    return {
        "rows_loaded": len(df),
        "table_id": table_id,
        "job_id": job.job_id
    }
