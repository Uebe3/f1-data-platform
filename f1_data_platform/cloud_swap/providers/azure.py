"""Azure cloud provider implementation."""

import os
from typing import Any, Dict, List, Optional, IO
import pandas as pd

try:
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
    import pyodbc
    from sqlalchemy import create_engine
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

from ..interfaces import StorageProvider, DatabaseProvider, ComputeProvider, CloudProvider


class AzureStorageProvider(StorageProvider):
    """Azure Blob Storage provider."""

    def __init__(self, account_name: str, container_name: str, 
                 account_key: Optional[str] = None, 
                 connection_string: Optional[str] = None,
                 use_credential: bool = True, **kwargs):
        if not AZURE_AVAILABLE:
            raise ImportError("Azure dependencies not installed. Run: pip install azure-storage-blob azure-identity")
        
        self.account_name = account_name
        self.container_name = container_name
        
        # Initialize blob service client with different auth methods
        if connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_key:
            account_url = f"https://{account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        elif use_credential:
            # Use Azure default credential (managed identity, service principal, etc.)
            credential = DefaultAzureCredential()
            account_url = f"https://{account_name}.blob.core.windows.net"
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            raise ValueError("Must provide either connection_string, account_key, or use_credential=True")
        
        # Get container client and create container if it doesn't exist
        self.container_client = self.blob_service_client.get_container_client(container_name)
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass  # Container already exists

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
            return True
        except Exception:
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from Azure Blob Storage."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, 'wb') as download_file:
                download_data = blob_client.download_blob()
                download_file.write(download_data.readall())
            return True
        except ResourceNotFoundError:
            return False
        except Exception:
            return False

    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Upload a pandas DataFrame to Azure Blob Storage."""
        try:
            import io
            
            blob_client = self.container_client.get_blob_client(remote_path)
            
            if format.lower() == "parquet":
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow")
                buffer.seek(0)
                blob_client.upload_blob(buffer, overwrite=True)
            elif format.lower() == "csv":
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                blob_client.upload_blob(
                    csv_buffer.getvalue().encode('utf-8'), 
                    overwrite=True,
                    content_type='text/csv'
                )
            elif format.lower() == "json":
                json_buffer = io.StringIO()
                df.to_json(json_buffer, orient="records")
                blob_client.upload_blob(
                    json_buffer.getvalue().encode('utf-8'), 
                    overwrite=True,
                    content_type='application/json'
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            return True
        except Exception:
            return False

    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Download a pandas DataFrame from Azure Blob Storage."""
        import io
        
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            download_data = blob_client.download_blob()
            data_bytes = download_data.readall()
            
            if format.lower() == "parquet":
                return pd.read_parquet(io.BytesIO(data_bytes), engine="pyarrow")
            elif format.lower() == "csv":
                return pd.read_csv(io.StringIO(data_bytes.decode("utf-8")))
            elif format.lower() == "json":
                return pd.read_json(io.StringIO(data_bytes.decode("utf-8")), orient="records")
            else:
                raise ValueError(f"Unsupported format: {format}")
        except ResourceNotFoundError:
            raise FileNotFoundError(f"File not found: {remote_path}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in Azure Blob Storage with optional prefix."""
        try:
            blob_list = self.container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blob_list]
        except Exception:
            return []

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.delete_blob()
            return True
        except ResourceNotFoundError:
            return False
        except Exception:
            return False

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception:
            return False

    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            properties = blob_client.get_blob_properties()
            return properties.size
        except ResourceNotFoundError:
            raise FileNotFoundError(f"File not found: {remote_path}")


class AzureDatabaseProvider(DatabaseProvider):
    """Azure SQL Database provider."""

    def __init__(self, server: str, database: str, username: str, password: str, 
                 port: int = 1433, driver: str = "ODBC Driver 17 for SQL Server", **kwargs):
        if not AZURE_AVAILABLE:
            raise ImportError("Azure dependencies not installed. Run: pip install pyodbc")
        
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.driver = driver
        
        # Build connection string for Azure SQL
        self.connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        # SQLAlchemy connection string for DataFrame operations
        self.sqlalchemy_url = (
            f"mssql+pyodbc:///{database}?"
            f"odbc_connect={self.connection_string.replace(';', '%3B').replace('=', '%3D').replace('{', '%7B').replace('}', '%7D')}"
        )
        
        self.connection = None
        self.engine = None

    def connect(self) -> Any:
        """Establish database connection."""
        try:
            if self.connection is None or self.connection.closed:
                self.connection = pyodbc.connect(self.connection_string)
            
            if self.engine is None:
                self.engine = create_engine(self.sqlalchemy_url)
            
            return self.connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Azure SQL Database: {e}")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a SQL query."""
        self.connect()
        cursor = self.connection.cursor()
        
        try:
            if params:
                # Convert dict params to ordered list for pyodbc
                param_values = list(params.values()) if isinstance(params, dict) else params
                cursor.execute(query, param_values)
            else:
                cursor.execute(query)
            
            # Check if this is a SELECT query
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                self.connection.commit()
                return cursor.rowcount
        finally:
            cursor.close()

    def execute_many(self, query: str, params: List[Dict]) -> Any:
        """Execute a SQL query with multiple parameter sets."""
        self.connect()
        cursor = self.connection.cursor()
        
        try:
            # Convert list of dicts to list of lists for pyodbc
            param_list = [list(param_dict.values()) for param_dict in params]
            cursor.executemany(query, param_list)
            self.connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()

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
        # Map common data types to SQL Server types
        type_mapping = {
            'int': 'INT',
            'integer': 'INT',
            'bigint': 'BIGINT',
            'float': 'FLOAT',
            'double': 'FLOAT',
            'varchar': 'NVARCHAR(255)',
            'text': 'NVARCHAR(MAX)',
            'datetime': 'DATETIME2',
            'date': 'DATE',
            'time': 'TIME',
            'boolean': 'BIT',
            'bool': 'BIT'
        }
        
        columns = []
        for column_name, column_type in schema.items():
            sql_type = type_mapping.get(column_type.lower(), column_type.upper())
            columns.append(f"[{column_name}] {sql_type}")
        
        create_query = f"CREATE TABLE [{table_name}] ({', '.join(columns)})"
        
        try:
            self.execute_query(create_query)
            return True
        except Exception:
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = ?
        """
        
        try:
            result = self.execute_query(query, [table_name])
            return result[0][0] > 0 if result else False
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        query = """
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            result = self.execute_query(query, [table_name])
            return {row[0]: row[1] for row in result} if result else {}
        except Exception:
            return {}

    def close(self) -> None:
        """Close the database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
        if self.engine:
            self.engine.dispose()


class AzureComputeProvider(ComputeProvider):
    """Azure Batch / Container Instances compute provider."""

    def __init__(self, batch_account_name: str = None, batch_account_url: str = None,
                 batch_account_key: str = None, resource_group: str = None,
                 subscription_id: str = None, **kwargs):
        if not AZURE_AVAILABLE:
            raise ImportError("Azure compute dependencies not installed. Run: pip install azure-batch azure-mgmt-containerinstance")
        
        self.batch_account_name = batch_account_name
        self.batch_account_url = batch_account_url
        self.batch_account_key = batch_account_key
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        
        # For now, implement a basic stub - full Azure Batch integration would require
        # more complex setup and configuration
        self.jobs = {}  # Simple in-memory job tracking for demo

    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a batch job and return job ID."""
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # In a real implementation, this would:
        # 1. Create Azure Batch job with specified configuration
        # 2. Submit tasks to the job
        # 3. Return the actual Azure job ID
        
        self.jobs[job_id] = {
            "status": "submitted",
            "config": job_config,
            "logs": f"Job {job_id} submitted successfully"
        }
        
        return job_id

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a batch job."""
        # In a real implementation, this would query Azure Batch service
        if job_id in self.jobs:
            return self.jobs[job_id]["status"]
        return "not_found"

    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a batch job."""
        # In a real implementation, this would retrieve logs from Azure Batch
        if job_id in self.jobs:
            return self.jobs[job_id]["logs"]
        return f"No logs found for job {job_id}"

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running batch job."""
        # In a real implementation, this would cancel the Azure Batch job
        if job_id in self.jobs:
            self.jobs[job_id]["status"] = "cancelled"
            return True
        return False


class AzureCloudProvider(CloudProvider):
    """Azure cloud provider implementation."""

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
            self._storage_provider = AzureStorageProvider(
                account_name=self.storage_config.get("account_name"),
                container_name=self.storage_config.get("container_name"),
                account_key=self.storage_config.get("account_key"),
                connection_string=self.storage_config.get("connection_string"),
                use_credential=self.storage_config.get("use_credential", True)
            )
        return self._storage_provider

    def get_database_provider(self) -> DatabaseProvider:
        """Get the database provider instance."""
        if self._database_provider is None:
            self._database_provider = AzureDatabaseProvider(
                server=self.database_config.get("server"),
                database=self.database_config.get("database"),
                username=self.database_config.get("username"),
                password=self.database_config.get("password"),
                port=self.database_config.get("port", 1433),
                driver=self.database_config.get("driver", "ODBC Driver 17 for SQL Server")
            )
        return self._database_provider

    def get_compute_provider(self) -> ComputeProvider:
        """Get the compute provider instance."""
        if self._compute_provider is None:
            self._compute_provider = AzureComputeProvider(
                batch_account_name=self.compute_config.get("batch_account_name"),
                batch_account_url=self.compute_config.get("batch_account_url"),
                batch_account_key=self.compute_config.get("batch_account_key"),
                resource_group=self.compute_config.get("resource_group"),
                subscription_id=self.compute_config.get("subscription_id")
            )
        return self._compute_provider

    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all services."""
        health_status = {}
        
        # Check storage
        try:
            storage = self.get_storage_provider()
            # Try to list blobs as a simple health check
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