"""Cloud provider abstraction interfaces."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, IO
import pandas as pd


class StorageProvider(ABC):
    """Abstract base class for cloud storage providers."""

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to cloud storage."""
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from cloud storage."""
        pass

    @abstractmethod
    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Upload a pandas DataFrame to cloud storage."""
        pass

    @abstractmethod
    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Download a file from cloud storage as pandas DataFrame."""
        pass

    @abstractmethod
    def list_files(self, prefix: str = "") -> List[str]:
        """List files in cloud storage with optional prefix."""
        pass

    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from cloud storage."""
        pass

    @abstractmethod
    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in cloud storage."""
        pass

    @abstractmethod
    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in cloud storage."""
        pass


class DatabaseProvider(ABC):
    """Abstract base class for database providers."""

    @abstractmethod
    def connect(self) -> Any:
        """Establish database connection."""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a SQL query."""
        pass

    @abstractmethod
    def execute_many(self, query: str, params: List[Dict]) -> Any:
        """Execute a SQL query with multiple parameter sets."""
        pass

    @abstractmethod
    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        pass

    @abstractmethod
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """Insert a pandas DataFrame into a database table."""
        pass

    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table with the specified schema."""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass


class ComputeProvider(ABC):
    """Abstract base class for compute providers (for future ML/batch processing)."""

    @abstractmethod
    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a batch job and return job ID."""
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """Get the status of a batch job."""
        pass

    @abstractmethod
    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a batch job."""
        pass

    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running batch job."""
        pass


class CloudProvider(ABC):
    """Abstract base class for complete cloud provider implementation."""

    @abstractmethod
    def get_storage_provider(self) -> StorageProvider:
        """Get the storage provider instance."""
        pass

    @abstractmethod
    def get_database_provider(self) -> DatabaseProvider:
        """Get the database provider instance."""
        pass

    @abstractmethod
    def get_compute_provider(self) -> ComputeProvider:
        """Get the compute provider instance."""
        pass

    @abstractmethod
    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all services."""
        pass