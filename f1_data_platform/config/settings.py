"""Configuration management for F1 Pipeline."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, validator


class StorageConfig(BaseModel):
    """Storage configuration."""
    provider: str = Field(..., description="Storage provider (local, aws, azure, gcp)")
    
    # Local storage
    local_path: Optional[str] = Field(None, description="Local storage path")
    
    # AWS S3
    bucket_name: Optional[str] = Field(None, description="S3 bucket name")
    region: Optional[str] = Field("us-east-1", description="AWS region")
    
    # Azure Blob Storage
    container_name: Optional[str] = Field(None, description="Azure container name")
    account_name: Optional[str] = Field(None, description="Azure storage account name")
    account_key: Optional[str] = Field(None, description="Azure storage account key")
    connection_string: Optional[str] = Field(None, description="Azure storage connection string")
    use_credential: Optional[bool] = Field(True, description="Use Azure default credential")
    
    # GCP Cloud Storage
    project_id: Optional[str] = Field(None, description="GCP project ID")
    credentials_path: Optional[str] = Field(None, description="Path to GCP service account credentials JSON")
    use_default_credentials: Optional[bool] = Field(True, description="Use Application Default Credentials")

    @validator('provider')
    def validate_provider(cls, v):
        allowed = ['local', 'aws', 'azure', 'gcp']
        if v.lower() not in allowed:
            raise ValueError(f'Provider must be one of {allowed}')
        return v.lower()


class DatabaseConfig(BaseModel):
    """Database configuration."""
    provider: str = Field(..., description="Database provider (local, aws_rds, azure_sql, gcp_sql, aws_athena)")
    
    # Local SQLite
    db_path: Optional[str] = Field(None, description="SQLite database path")
    
    # Cloud databases
    endpoint: Optional[str] = Field(None, description="Database endpoint")
    server: Optional[str] = Field(None, description="Database server (Azure SQL)")
    instance_connection_name: Optional[str] = Field(None, description="GCP Cloud SQL instance connection name (project:region:instance)")
    db_type: Optional[str] = Field("postgresql", description="GCP Cloud SQL database type (postgresql or mysql)")
    database: Optional[str] = Field(None, description="Database name")
    username: Optional[str] = Field(None, description="Database username")
    password: Optional[str] = Field(None, description="Database password")
    port: Optional[int] = Field(5432, description="Database port")
    driver: Optional[str] = Field("ODBC Driver 17 for SQL Server", description="ODBC driver for Azure SQL")
    
    # AWS Athena (Modern)
    athena_workgroup: Optional[str] = Field(None, description="Athena workgroup name")
    athena_database: Optional[str] = Field(None, description="Athena/Glue database name")
    athena_output_location: Optional[str] = Field(None, description="S3 location for Athena query results")

    @validator('provider')
    def validate_provider(cls, v):
        allowed = ['local', 'aws_rds', 'azure_sql', 'gcp_sql', 'aws_athena']
        if v.lower() not in allowed:
            raise ValueError(f'Provider must be one of {allowed}')
        return v.lower()


class OpenF1Config(BaseModel):
    """OpenF1 API configuration."""
    base_url: str = Field("https://api.openf1.org/v1", description="OpenF1 API base URL")
    rate_limit_delay: float = Field(0.1, description="Delay between API calls (seconds)")
    max_retries: int = Field(3, description="Maximum number of retries for failed requests")
    timeout: int = Field(30, description="Request timeout in seconds")
    years_to_fetch: list = Field([2019, 2020, 2021, 2022, 2023], description="Years to fetch data for")


class ComputeConfig(BaseModel):
    """Compute configuration for batch processing."""
    provider: Optional[str] = Field("local", description="Compute provider (local, aws_batch, azure_batch, gcp_batch, aws_lambda)")
    
    # AWS Batch
    job_queue: Optional[str] = Field(None, description="AWS Batch job queue")
    job_definition: Optional[str] = Field(None, description="AWS Batch job definition")
    
    # AWS Lambda (Modern)
    lambda_function_name: Optional[str] = Field(None, description="Lambda function name for data processing")
    lambda_timeout: Optional[int] = Field(900, description="Lambda function timeout in seconds")
    lambda_memory: Optional[int] = Field(1024, description="Lambda function memory in MB")
    
    # Azure Batch
    batch_account_name: Optional[str] = Field(None, description="Azure Batch account name")
    batch_account_url: Optional[str] = Field(None, description="Azure Batch account URL")
    batch_account_key: Optional[str] = Field(None, description="Azure Batch account key")
    resource_group: Optional[str] = Field(None, description="Azure resource group")
    subscription_id: Optional[str] = Field(None, description="Azure subscription ID")
    
    # GCP Batch/Cloud Run
    project_id: Optional[str] = Field(None, description="GCP project ID")
    region: Optional[str] = Field("us-central1", description="GCP region")
    service_account_email: Optional[str] = Field(None, description="GCP service account email")
    credentials_path: Optional[str] = Field(None, description="Path to GCP service account credentials JSON")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field("INFO", description="Logging level")
    format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    file_path: Optional[str] = Field(None, description="Log file path")


class Settings(BaseModel):
    """Main configuration settings."""
    environment: str = Field("local", description="Environment (local, aws, azure, gcp)")
    storage: StorageConfig
    database: DatabaseConfig
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    openf1: OpenF1Config = Field(default_factory=OpenF1Config)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    class Config:
        env_prefix = "F1_PIPELINE_"
        case_sensitive = False

    @classmethod
    def load_from_file(cls, config_path: str) -> 'Settings':
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)

    @classmethod
    def load_from_env(cls) -> 'Settings':
        """Load configuration from environment variables."""
        # This would need more complex logic to handle nested configs
        # For now, just return default local config
        return cls(
            environment="local",
            storage=StorageConfig(provider="local", local_path="./local-data"),
            database=DatabaseConfig(provider="local", db_path="./local-data/f1_pipeline.db")
        )

    def save_to_file(self, config_path: str) -> None:
        """Save configuration to YAML file."""
        config_file = Path(config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_file, 'w') as f:
            yaml.dump(self.dict(), f, default_flow_style=False, indent=2)

    def get_cloud_provider_config(self) -> Dict[str, Any]:
        """Get cloud provider configuration for factory."""
        config = {
            "storage": self.storage.dict(),
            "database": self.database.dict(),
            "compute": self.compute.dict(),
        }
        
        # Add environment-specific configs
        if self.environment == "aws":
            config.update({
                "region": self.storage.region
            })
        elif self.environment == "aws_modern":
            config.update({
                "region": self.storage.region,
                "athena_workgroup": self.database.athena_workgroup,
                "athena_database": self.database.athena_database,
                "athena_output_location": self.database.athena_output_location
            })
        elif self.environment == "azure":
            config.update({
                "resource_group": self.compute.resource_group,
                "subscription_id": self.compute.subscription_id
            })
        elif self.environment == "gcp":
            config.update({
                "project_id": self.storage.project_id,
                "region": self.compute.region,
                "credentials_path": self.storage.credentials_path or self.compute.credentials_path
            })
        
        return config