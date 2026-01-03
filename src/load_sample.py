"""Load sample campaign data to BigQuery bronze table."""

import logging
import io
import polars as pl
from pathlib import Path
from google.cloud import bigquery
import google.auth

from config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Load sample CSV to BigQuery bronze table."""
    config = load_config()

    # Load sample data
    data_path = Path("data/campaigns.csv")
    df = pl.read_csv(data_path)
    logger.info(f"Loaded {len(df)} campaigns from {data_path}")

    # Connect to BigQuery
    credentials, project = google.auth.default()
    client = bigquery.Client(
        credentials=credentials,
        project=config.source.project or project
    )

    # Create dataset if not exists
    dataset_ref = client.dataset(config.source.dataset)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset: {config.source.dataset}")

    # Load to BigQuery
    table_id = f"{client.project}.{config.source.dataset}.{config.source.table}"

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


if __name__ == "__main__":
    main()
