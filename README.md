# AI Pipeline Template

Standardize messy campaign names using Claude. Same ETL pattern as data and ML pipelines.

## Quick Start

**1. Install Google Cloud SDK:**

```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
```

**2. Install hatch:**

```bash
sudo apt-get install pipx
pipx install hatch
```

**3. Set up credentials:**

```bash
cp .env.example .env
# Edit .env with your Anthropic API key
```

Set project before authenticating:

```bash
gcloud config set project your_project
gcloud auth application-default login
```

**4. Run:**

```bash
hatch run load      # Load sample data to BigQuery
hatch run pipeline  # Parse campaign names with Claude
hatch run analyze   # Show metrics by platform/objective/geo
```

## Input / Output

**Input:**
```
campaign_name              | spend   | revenue
fb_us_prosp_2024           | 850.00  | 2100.00
Facebook - Prospecting US  | 720.00  | 1850.00
meta|cold|usa|nov          | 920.00  | 2400.00
```

**Output:**
```
campaign_name     | spend  | revenue | source   | objective   | geo
fb_us_prosp_2024  | 850.00 | 2100.00 | facebook | prospecting | US
Facebook - Prosp… | 720.00 | 1850.00 | facebook | prospecting | US
meta|cold|usa|nov | 920.00 | 2400.00 | facebook | null        | US
```

Now you can `GROUP BY source` across inconsistent naming.

## Batch Processing

- 100 campaigns per API call (auto-chunks larger datasets)
- 1000 campaigns = 10 API calls, not 1000

## Structure

```
ai-pipeline-template/
├── data/campaigns.csv      # Sample data
├── src/
│   ├── extract.py          # Read from BigQuery
│   ├── transform.py        # Clean data
│   ├── generate.py         # Call Claude (batch)
│   ├── load.py             # Write to BigQuery
│   ├── pipeline.py         # Orchestrate
│   └── analysis.py         # Query results
├── config.yaml
└── pyproject.toml
```

## Pattern

| Step         | Data Pipeline     | ML Pipeline       | AI Pipeline      |
| ------------ | ----------------- | ----------------- | ---------------- |
| extract.py   | Source → DataFrame | Source → DataFrame | BigQuery → DataFrame |
| transform.py | Clean & aggregate | Feature engineering | Clean data |
| **middle**   | —                 | model.py          | generate.py (LLM) |
| load.py      | DataFrame → Dest  | DataFrame → Dest  | DataFrame → BigQuery |
