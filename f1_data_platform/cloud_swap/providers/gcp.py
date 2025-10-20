"""Google Cloud Platform (GCP) cloud provider implementation."""

import os
from typing import Any, Dict, List, Optional, IO
import pandas as pd

try:
    from google.cloud import storage
    from google.auth import default
    from google.oauth2 import service_account
    from google.cloud.exceptions import NotFound, Forbidden
    import psycopg2
    from sqlalchemy import create_engine
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False

from ..interfaces import StorageProvider, DatabaseProvider, ComputeProvider, CloudProvider


class GCPStorageProvider(StorageProvider):
    """Google Cloud Storage provider."""

    def __init__(self, bucket_name: str, project_id: str,
                 credentials_path: Optional[str] = None,
                 use_default_credentials: bool = True, **kwargs):
        if not GCP_AVAILABLE:
            raise ImportError("GCP dependencies not installed. Run: pip install google-cloud-storage google-auth")
        
        self.bucket_name = bucket_name
        self.project_id = project_id
        
        # Initialize client with different auth methods
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = storage.Client(project=project_id, credentials=credentials)
        elif use_default_credentials:
            # Use Application Default Credentials (ADC)
            self.client = storage.Client(project=project_id)
        else:
            raise ValueError("Must provide either credentials_path or use_default_credentials=True")
        
        # Get bucket and create if it doesn't exist
        try:
            self.bucket = self.client.bucket(bucket_name)
            # Check if bucket exists by trying to get its metadata
            self.bucket.reload()
        except NotFound:
            # Create bucket if it doesn't exist
            self.bucket = self.client.create_bucket(bucket_name, project=project_id)

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.upload_from_filename(local_path)
            return True
        except Exception:
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from Google Cloud Storage."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob = self.bucket.blob(remote_path)
            blob.download_to_filename(local_path)
            return True
        except NotFound:
            return False
        except Exception:
            return False

    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Upload a pandas DataFrame to Google Cloud Storage."""
        try:
            import io
            
            blob = self.bucket.blob(remote_path)
            
            if format.lower() == "parquet":
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow")
                buffer.seek(0)
                blob.upload_from_file(buffer, content_type='application/octet-stream')
            elif format.lower() == "csv":
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                blob.upload_from_string(
                    csv_buffer.getvalue(),
                    content_type='text/csv'
                )
            elif format.lower() == "json":
                json_buffer = io.StringIO()
                df.to_json(json_buffer, orient="records")
                blob.upload_from_string(
                    json_buffer.getvalue(),
                    content_type='application/json'
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            return True
        except Exception:
            return False

    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Download a pandas DataFrame from Google Cloud Storage."""
        import io
        
        try:
            blob = self.bucket.blob(remote_path)
            data = blob.download_as_bytes()
            
            if format.lower() == "parquet":
                return pd.read_parquet(io.BytesIO(data), engine="pyarrow")
            elif format.lower() == "csv":
                return pd.read_csv(io.StringIO(data.decode("utf-8")))
            elif format.lower() == "json":
                return pd.read_json(io.StringIO(data.decode("utf-8")), orient="records")
            else:
                raise ValueError(f"Unsupported format: {format}")
        except NotFound:
            raise FileNotFoundError(f"File not found: {remote_path}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in Google Cloud Storage with optional prefix."""
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception:
            return []

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.delete()
            return True
        except NotFound:
            return False
        except Exception:
            return False

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            return blob.exists()
        except Exception:
            return False

    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.reload()  # Refresh metadata
            return blob.size
        except NotFound:
            raise FileNotFoundError(f"File not found: {remote_path}")


class GCPDatabaseProvider(DatabaseProvider):
    """Google Cloud SQL (PostgreSQL/MySQL) database provider."""

    def __init__(self, instance_connection_name: str, database: str, username: str, password: str,
                 db_type: str = "postgresql", port: int = 5432, **kwargs):
        if not GCP_AVAILABLE:
            raise ImportError("GCP database dependencies not installed. Run: pip install psycopg2-binary cloud-sql-python-connector")
        
        self.instance_connection_name = instance_connection_name  # project:region:instance
        self.database = database
        self.username = username
        self.password = password
        self.db_type = db_type.lower()
        self.port = port
        
        # Build connection string for Cloud SQL
        if self.db_type == "postgresql":
            # For Cloud SQL PostgreSQL
            self.connection_string = f"postgresql://{username}:{password}@/{database}?host=/cloudsql/{instance_connection_name}"
            self.sqlalchemy_url = self.connection_string
        elif self.db_type == "mysql":
            # For Cloud SQL MySQL
            self.connection_string = f"mysql+pymysql://{username}:{password}@/{database}?unix_socket=/cloudsql/{instance_connection_name}"
            self.sqlalchemy_url = self.connection_string
        else:
            raise ValueError(f"Unsupported database type: {db_type}. Use 'postgresql' or 'mysql'")
        
        self.connection = None
        self.engine = None

    def connect(self) -> Any:
        """Establish database connection."""
        try:
            if self.engine is None:
                self.engine = create_engine(self.sqlalchemy_url)
            
            if self.connection is None or self.connection.closed:
                self.connection = self.engine.connect()
            
            return self.connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Google Cloud SQL: {e}")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a SQL query."""
        self.connect()
        
        try:
            if params:
                result = self.connection.execute(query, params)
            else:
                result = self.connection.execute(query)
            
            # Check if this is a SELECT query
            if query.strip().upper().startswith('SELECT'):
                return result.fetchall()
            else:
                self.connection.commit()
                return result.rowcount
        except Exception as e:
            self.connection.rollback()
            raise e

    def execute_many(self, query: str, params: List[Dict]) -> Any:
        """Execute a SQL query with multiple parameter sets."""
        self.connect()
        
        try:
            result = self.connection.execute(query, params)
            self.connection.commit()
            return result.rowcount
        except Exception as e:
            self.connection.rollback()
            raise e

    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        self.connect()
        
        try:
            if params:
                return pd.read_sql(query, self.engine, params=params)
            else:
                return pd.read_sql(query, self.engine)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch DataFrame: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """Insert a pandas DataFrame into a database table."""
        self.connect()
        
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, method='multi')
            return True
        except Exception:
            return False

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table with the specified schema."""
        # Map common data types to PostgreSQL/MySQL types
        if self.db_type == "postgresql":
            type_mapping = {
                'int': 'INTEGER',
                'integer': 'INTEGER',
                'bigint': 'BIGINT',
                'float': 'REAL',
                'double': 'DOUBLE PRECISION',
                'varchar': 'VARCHAR(255)',
                'text': 'TEXT',
                'datetime': 'TIMESTAMP',
                'date': 'DATE',
                'time': 'TIME',
                'boolean': 'BOOLEAN',
                'bool': 'BOOLEAN'
            }
        else:  # mysql
            type_mapping = {
                'int': 'INT',
                'integer': 'INT',
                'bigint': 'BIGINT',
                'float': 'FLOAT',
                'double': 'DOUBLE',
                'varchar': 'VARCHAR(255)',
                'text': 'TEXT',
                'datetime': 'DATETIME',
                'date': 'DATE',
                'time': 'TIME',
                'boolean': 'BOOLEAN',
                'bool': 'BOOLEAN'
            }
        
        columns = []
        for column_name, column_type in schema.items():
            sql_type = type_mapping.get(column_type.lower(), column_type.upper())
            columns.append(f"`{column_name}` {sql_type}")
        
        create_query = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
        
        try:
            self.execute_query(create_query)
            return True
        except Exception:
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if self.db_type == "postgresql":
            query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = %s
            """
        else:  # mysql
            query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = %s
            """
        
        try:
            result = self.execute_query(query, [table_name])
            return result[0][0] > 0 if result else False
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        if self.db_type == "postgresql":
            query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
        else:  # mysql
            query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
        
        try:
            result = self.execute_query(query, [table_name])
            return {row[0]: row[1] for row in result} if result else {}
        except Exception:
            return {}

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()


class GCPComputeProvider(ComputeProvider):
    """Google Cloud Run / Cloud Functions / Cloud Batch compute provider."""

    def __init__(self, project_id: str, region: str = "us-central1",
                 service_account_email: Optional[str] = None,
                 credentials_path: Optional[str] = None, **kwargs):
        if not GCP_AVAILABLE:
            raise ImportError("GCP compute dependencies not installed. Run: pip install google-cloud-run google-cloud-functions")
        
        self.project_id = project_id
        self.region = region
        self.service_account_email = service_account_email
        self.credentials_path = credentials_path
        
        # For now, implement a basic stub - full GCP compute integration would require
        # more complex setup and configuration
        self.jobs = {}  # Simple in-memory job tracking for demo

    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a compute job and return job ID."""
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # In a real implementation, this would:
        # 1. Create Cloud Run job or Cloud Function
        # 2. Submit the job with specified configuration
        # 3. Return the actual GCP job ID
        
        self.jobs[job_id] = {
            "status": "submitted",
            "config": job_config,
            "logs": f"Job {job_id} submitted to GCP successfully",
            "project_id": self.project_id,
            "region": self.region
        }
        
        return job_id

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a compute job."""
        # In a real implementation, this would query GCP Cloud Run or Cloud Functions
        if job_id in self.jobs:
            return self.jobs[job_id]["status"]
        return "not_found"

    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a compute job."""
        # In a real implementation, this would retrieve logs from GCP Cloud Logging
        if job_id in self.jobs:
            return self.jobs[job_id]["logs"]
        return f"No logs found for job {job_id}"

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running compute job."""
        # In a real implementation, this would cancel the GCP job
        if job_id in self.jobs:
            self.jobs[job_id]["status"] = "cancelled"
            return True
        return False


class GCPCloudProvider(CloudProvider):
    """Google Cloud Platform cloud provider implementation."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage_config = config.get("storage", {})
        self.database_config = config.get("database", {})
        self.compute_config = config.get("compute", {})
        
        # Initialize providers lazily
        self._storage_provider = None
        self._database_provider = None
        self._compute_provider = None

    def get_storage_provider(self) -> StorageProvider:
        """Get the storage provider instance."""
        if self._storage_provider is None:
            self._storage_provider = GCPStorageProvider(
                bucket_name=self.storage_config.get("bucket_name"),
                project_id=self.storage_config.get("project_id"),
                credentials_path=self.storage_config.get("credentials_path"),
                use_default_credentials=self.storage_config.get("use_default_credentials", True)
            )
        return self._storage_provider

    def get_database_provider(self) -> DatabaseProvider:
        """Get the database provider instance."""
        if self._database_provider is None:
            self._database_provider = GCPDatabaseProvider(
                instance_connection_name=self.database_config.get("instance_connection_name"),
                database=self.database_config.get("database"),
                username=self.database_config.get("username"),
                password=self.database_config.get("password"),
                db_type=self.database_config.get("db_type", "postgresql"),
                port=self.database_config.get("port", 5432)
            )
        return self._database_provider

    def get_compute_provider(self) -> ComputeProvider:
        """Get the compute provider instance."""
        if self._compute_provider is None:
            self._compute_provider = GCPComputeProvider(
                project_id=self.compute_config.get("project_id"),
                region=self.compute_config.get("region", "us-central1"),
                service_account_email=self.compute_config.get("service_account_email"),
                credentials_path=self.compute_config.get("credentials_path")
            )
        return self._compute_provider

    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all services."""
        health_status = {}
        
        # Check storage
        try:
            storage = self.get_storage_provider()
            # Try to list objects as a simple health check
            storage.list_files()
            health_status["storage"] = True
        except Exception:
            health_status["storage"] = False
        
        # Check database
        try:
            database = self.get_database_provider()
            database.connect()
            health_status["database"] = True
        except Exception:
            health_status["database"] = False
        
        # Check compute (basic check since it's a stub implementation)
        try:
            compute = self.get_compute_provider()
            health_status["compute"] = True
        except Exception:
            health_status["compute"] = False
        
        return health_status