"""Configuration models with validation."""

import yaml
import os
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()


class SourceConfig(BaseModel):
    """BigQuery source configuration."""
    project: str
    dataset: str
    table: str


class DestinationConfig(BaseModel):
    """BigQuery destination configuration."""
    project: str
    dataset: str
    table: str


class ModelConfig(BaseModel):
    """LLM configuration."""
    provider: str = "anthropic"
    model_name: str = "claude-sonnet-4-20250514"
    prompt: str


class PipelineConfig(BaseModel):
    """Complete pipeline configuration."""
    name: str
    version: str


class Config(BaseModel):
    """Root configuration."""
    pipeline: PipelineConfig
    source: SourceConfig
    destination: DestinationConfig
    model: ModelConfig


def load_config(path: str = "config.yaml") -> Config:
    """Load and validate configuration from YAML."""
    with open(path) as f:
        config_dict = yaml.safe_load(f)

    # Inject project from environment if not set
    project_id = os.getenv("GCP_PROJECT_ID", "")
    if project_id:
        config_dict["source"]["project"] = project_id
        config_dict["destination"]["project"] = project_id

    return Config(**config_dict)
