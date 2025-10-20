"""Local filesystem provider implementation."""

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, IO
import pandas as pd

from ..interfaces import StorageProvider, DatabaseProvider, ComputeProvider, CloudProvider


class LocalStorageProvider(StorageProvider):
    """Local filesystem storage provider."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_full_path(self, remote_path: str) -> Path:
        """Get full local path for a remote path."""
        return self.base_path / remote_path.lstrip("/")

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Copy a file to local storage."""
        try:
            full_path = self._get_full_path(remote_path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, full_path)
            return True
        except Exception:
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Copy a file from local storage."""
        try:
            full_path = self._get_full_path(remote_path)
            if not full_path.exists():
                return False
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.copy2(full_path, local_path)
            return True
        except Exception:
            return False

    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Save a pandas DataFrame to local storage."""
        try:
            full_path = self._get_full_path(remote_path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            if format.lower() == "parquet":
                df.to_parquet(full_path, engine="pyarrow")
            elif format.lower() == "csv":
                df.to_csv(full_path, index=False)
            elif format.lower() == "json":
                df.to_json(full_path, orient="records")
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            return True
        except Exception:
            return False

    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Load a pandas DataFrame from local storage."""
        full_path = self._get_full_path(remote_path)
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {remote_path}")
        
        if format.lower() == "parquet":
            return pd.read_parquet(full_path, engine="pyarrow")
        elif format.lower() == "csv":
            return pd.read_csv(full_path)
        elif format.lower() == "json":
            return pd.read_json(full_path, orient="records")
        else:
            raise ValueError(f"Unsupported format: {format}")

    def list_files(self, prefix: str = "") -> List[str]:
        """List files with optional prefix."""
        prefix_path = self._get_full_path(prefix) if prefix else self.base_path
        
        if not prefix_path.exists():
            return []
        
        files = []
        if prefix_path.is_file():
            return [str(prefix_path.relative_to(self.base_path))]
        
        for item in prefix_path.rglob("*"):
            if item.is_file():
                rel_path = item.relative_to(self.base_path)
                files.append(str(rel_path).replace("\\", "/"))  # Normalize path separators
        
        return sorted(files)

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from local storage."""
        try:
            full_path = self._get_full_path(remote_path)
            if full_path.exists():
                full_path.unlink()
                return True
            return False
        except Exception:
            return False

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in local storage."""
        full_path = self._get_full_path(remote_path)
        return full_path.exists() and full_path.is_file()

    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in local storage."""
        full_path = self._get_full_path(remote_path)
        if full_path.exists() and full_path.is_file():
            return full_path.stat().st_size
        return 0


class LocalDatabaseProvider(DatabaseProvider):
    """SQLite database provider for local development."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        """Establish SQLite connection."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            # Enable foreign key constraints
            self.connection.execute("PRAGMA foreign_keys = ON")
        return self.connection

    def execute_query(self, query: str, params: Optional[Dict] = None) -> sqlite3.Cursor:
        """Execute a SQL query."""
        conn = self.connect()
        if params:
            return conn.execute(query, params)
        return conn.execute(query)

    def execute_many(self, query: str, params: List[Dict]) -> sqlite3.Cursor:
        """Execute a SQL query with multiple parameter sets."""
        conn = self.connect()
        return conn.executemany(query, params)

    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        conn = self.connect()
        return pd.read_sql_query(query, conn, params=params)

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """Insert a pandas DataFrame into a database table."""
        try:
            conn = self.connect()
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.commit()
            return True
        except Exception:
            return False

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create a table with the specified schema."""
        try:
            columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.execute_query(query)
            self.connect().commit()
            return True
        except Exception:
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            result = self.execute_query(query, {"name": table_name}).fetchone()
            return result is not None
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get the schema of a table."""
        try:
            query = f"PRAGMA table_info({table_name})"
            result = self.execute_query(query).fetchall()
            return {row[1]: row[2] for row in result}
        except Exception:
            return {}

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None


class LocalComputeProvider(ComputeProvider):
    """Local compute provider (placeholder for future implementation)."""

    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a local job (placeholder)."""
        raise NotImplementedError("Local compute jobs not yet implemented")

    def get_job_status(self, job_id: str) -> str:
        """Get job status (placeholder)."""
        raise NotImplementedError("Local compute jobs not yet implemented")

    def get_job_logs(self, job_id: str) -> str:
        """Get job logs (placeholder)."""
        raise NotImplementedError("Local compute jobs not yet implemented")

    def cancel_job(self, job_id: str) -> bool:
        """Cancel job (placeholder)."""
        raise NotImplementedError("Local compute jobs not yet implemented")


class LocalCloudProvider(CloudProvider):
    """Local development cloud provider."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_path = config.get("base_path", "./local-data")
        self.db_path = os.path.join(self.base_path, "f1_pipeline.db")
        
        self._storage_provider = None
        self._database_provider = None
        self._compute_provider = None

    def get_storage_provider(self) -> StorageProvider:
        """Get the local storage provider instance."""
        if self._storage_provider is None:
            self._storage_provider = LocalStorageProvider(self.base_path)
        return self._storage_provider

    def get_database_provider(self) -> DatabaseProvider:
        """Get the local database provider instance."""
        if self._database_provider is None:
            self._database_provider = LocalDatabaseProvider(self.db_path)
        return self._database_provider

    def get_compute_provider(self) -> ComputeProvider:
        """Get the local compute provider instance."""
        if self._compute_provider is None:
            self._compute_provider = LocalComputeProvider()
        return self._compute_provider

    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all services."""
        health = {}
        
        # Check storage
        try:
            storage = self.get_storage_provider()
            test_file = "health_check_test.txt"
            os.makedirs(self.base_path, exist_ok=True)
            with open(os.path.join(self.base_path, test_file), "w") as f:
                f.write("health check")
            health["storage"] = storage.file_exists(test_file)
            storage.delete_file(test_file)
        except Exception:
            health["storage"] = False
        
        # Check database
        try:
            db = self.get_database_provider()
            db.connect()
            db.execute_query("SELECT 1")
            health["database"] = True
        except Exception:
            health["database"] = False
        
        # Compute is not implemented locally
        health["compute"] = False
        
        return health