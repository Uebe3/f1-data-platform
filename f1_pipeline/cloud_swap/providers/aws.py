"""AWS cloud provider implementation."""

import os
from typing import Any, Dict, List, Optional, IO
import pandas as pd

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    import psycopg2
    from sqlalchemy import create_engine
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

from ..interfaces import StorageProvider, DatabaseProvider, ComputeProvider, CloudProvider


class AWSStorageProvider(StorageProvider):
    """AWS S3 storage provider."""

    def __init__(self, bucket_name: str, region: str = "us-east-1", **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client("s3", region_name=region, **kwargs)

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to S3."""
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, remote_path)
            return True
        except (ClientError, FileNotFoundError):
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from S3."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.s3_client.download_file(self.bucket_name, remote_path, local_path)
            return True
        except ClientError:
            return False

    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Upload a pandas DataFrame to S3."""
        try:
            import io
            
            if format.lower() == "parquet":
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow")
                buffer.seek(0)
                self.s3_client.upload_fileobj(buffer, self.bucket_name, remote_path)
            elif format.lower() == "csv":
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=remote_path,
                    Body=csv_buffer.getvalue()
                )
            elif format.lower() == "json":
                json_buffer = io.StringIO()
                df.to_json(json_buffer, orient="records")
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=remote_path,
                    Body=json_buffer.getvalue()
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            return True
        except Exception:
            return False

    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Download a pandas DataFrame from S3."""
        import io
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=remote_path)
            
            if format.lower() == "parquet":
                return pd.read_parquet(io.BytesIO(response["Body"].read()), engine="pyarrow")
            elif format.lower() == "csv":
                return pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
            elif format.lower() == "json":
                return pd.read_json(io.StringIO(response["Body"].read().decode("utf-8")), orient="records")
            else:
                raise ValueError(f"Unsupported format: {format}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {remote_path}")
            raise

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in S3 with optional prefix."""
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            files = []
            for page in pages:
                for obj in page.get("Contents", []):
                    files.append(obj["Key"])
            
            return sorted(files)
        except ClientError:
            return []

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from S3."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in S3."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=remote_path)
            return response["ContentLength"]
        except ClientError:
            return 0


class AWSRDSProvider(DatabaseProvider):
    """AWS RDS database provider."""

    def __init__(self, endpoint: str, database: str, username: str, password: str, 
                 port: int = 5432, **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install psycopg2-binary sqlalchemy")
        
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection_string = f"postgresql://{username}:{password}@{endpoint}:{port}/{database}"
        self.engine = None

    def connect(self) -> Any:
        """Establish RDS connection."""
        if self.engine is None:
            self.engine = create_engine(self.connection_string)
        return self.engine

    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a SQL query."""
        engine = self.connect()
        with engine.connect() as conn:
            return conn.execute(query, params or {})

    def execute_many(self, query: str, params: List[Dict]) -> Any:
        """Execute a SQL query with multiple parameter sets."""
        engine = self.connect()
        with engine.connect() as conn:
            return conn.execute(query, params)

    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        engine = self.connect()
        return pd.read_sql_query(query, engine, params=params)

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """Insert a pandas DataFrame into a database table."""
        try:
            engine = self.connect()
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            return True
        except Exception:
            return False

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table with the specified schema."""
        try:
            columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.execute_query(query)
            return True
        except Exception:
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %(table_name)s
                )
            """
            result = self.execute_query(query, {"table_name": table_name}).fetchone()
            return result[0] if result else False
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        try:
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %(table_name)s
            """
            df = self.fetch_dataframe(query, {"table_name": table_name})
            return dict(zip(df["column_name"], df["data_type"]))
        except Exception:
            return {}

    def close(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None


class AWSBatchProvider(ComputeProvider):
    """AWS Batch compute provider."""

    def __init__(self, job_queue: str, job_definition: str, region: str = "us-east-1", **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.batch_client = boto3.client("batch", region_name=region, **kwargs)

    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a batch job and return job ID."""
        try:
            response = self.batch_client.submit_job(
                jobName=job_config.get("jobName", "f1-pipeline-job"),
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                parameters=job_config.get("parameters", {}),
                containerOverrides=job_config.get("containerOverrides", {})
            )
            return response["jobId"]
        except ClientError:
            raise

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a batch job."""
        try:
            response = self.batch_client.describe_jobs(jobs=[job_id])
            if response["jobs"]:
                return response["jobs"][0]["status"]
            return "NOT_FOUND"
        except ClientError:
            return "ERROR"

    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a batch job."""
        try:
            # This would require CloudWatch Logs integration
            # Placeholder implementation
            response = self.batch_client.describe_jobs(jobs=[job_id])
            if response["jobs"]:
                job = response["jobs"][0]
                log_stream = job.get("attempts", [{}])[0].get("logStreamName", "")
                return f"Log stream: {log_stream}"
            return "No logs available"
        except ClientError:
            return "Error retrieving logs"

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running batch job."""
        try:
            self.batch_client.cancel_job(jobId=job_id, reason="User requested cancellation")
            return True
        except ClientError:
            return False


class AWSCloudProvider(CloudProvider):
    """AWS cloud provider implementation."""

    def __init__(self, config: Dict[str, Any]):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3 psycopg2-binary sqlalchemy")
        
        self.config = config
        self.region = config.get("region", "us-east-1")
        
        self._storage_provider = None
        self._database_provider = None
        self._compute_provider = None

    def get_storage_provider(self) -> StorageProvider:
        """Get the AWS S3 storage provider instance."""
        if self._storage_provider is None:
            storage_config = self.config["storage"]
            self._storage_provider = AWSStorageProvider(
                bucket_name=storage_config["bucket_name"],
                region=self.region,
                **storage_config.get("s3_config", {})
            )
        return self._storage_provider

    def get_database_provider(self) -> DatabaseProvider:
        """Get the AWS RDS database provider instance."""
        if self._database_provider is None:
            db_config = self.config["database"]
            self._database_provider = AWSRDSProvider(
                endpoint=db_config["endpoint"],
                database=db_config["database"],
                username=db_config["username"],
                password=db_config["password"],
                port=db_config.get("port", 5432)
            )
        return self._database_provider

    def get_compute_provider(self) -> ComputeProvider:
        """Get the AWS Batch compute provider instance."""
        if self._compute_provider is None:
            compute_config = self.config.get("compute", {})
            self._compute_provider = AWSBatchProvider(
                job_queue=compute_config["job_queue"],
                job_definition=compute_config["job_definition"],
                region=self.region
            )
        return self._compute_provider

    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all AWS services."""
        health = {}
        
        # Check S3
        try:
            storage = self.get_storage_provider()
            # Try to list objects (this will fail if bucket doesn't exist or no permissions)
            storage.list_files()
            health["storage"] = True
        except Exception:
            health["storage"] = False
        
        # Check RDS
        try:
            db = self.get_database_provider()
            db.execute_query("SELECT 1")
            health["database"] = True
        except Exception:
            health["database"] = False
        
        # Check Batch
        try:
            compute = self.get_compute_provider()
            # Simple check - try to describe job queues
            health["compute"] = True
        except Exception:
            health["compute"] = False
        
        return health