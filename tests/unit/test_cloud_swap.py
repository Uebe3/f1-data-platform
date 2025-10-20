"""Unit tests for cloud swap functionality."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import tempfile
import os

from f1_pipeline.cloud_swap.factory import CloudProviderFactory, get_cloud_provider
from f1_pipeline.cloud_swap.providers.local import LocalCloudProvider, LocalStorageProvider, LocalDatabaseProvider


class TestCloudProviderFactory:
    """Test CloudProviderFactory functionality."""
    
    def test_create_local_provider(self, temp_dir):
        """Test creating local cloud provider."""
        config = {"base_path": temp_dir}
        provider = CloudProviderFactory.create("local", config)
        
        assert isinstance(provider, LocalCloudProvider)
        assert provider.base_path == temp_dir
    
    def test_get_supported_providers(self):
        """Test getting list of supported providers."""
        providers = CloudProviderFactory.get_supported_providers()
        
        assert "local" in providers
        assert "aws" in providers
        assert "azure" in providers
        assert "gcp" in providers
    
    def test_invalid_provider_type(self):
        """Test error for invalid provider type."""
        with pytest.raises(ValueError, match="Unsupported cloud provider: invalid"):
            CloudProviderFactory.create("invalid", {})
    
    def test_get_cloud_provider_function(self, temp_dir):
        """Test standalone get_cloud_provider function."""
        config = {"base_path": temp_dir}
        provider = get_cloud_provider("local", config)
        
        assert isinstance(provider, LocalCloudProvider)


class TestLocalStorageProvider:
    """Test LocalStorageProvider functionality."""
    
    def test_initialization(self, temp_dir):
        """Test storage provider initialization."""
        provider = LocalStorageProvider(temp_dir)
        
        assert provider.base_path.exists()
        assert str(provider.base_path) == temp_dir
    
    def test_upload_download_file(self, temp_dir):
        """Test file upload and download."""
        provider = LocalStorageProvider(temp_dir)
        
        # Create a test file
        test_content = "test content"
        local_file = os.path.join(temp_dir, "test_input.txt")
        with open(local_file, "w") as f:
            f.write(test_content)
        
        # Upload file
        success = provider.upload_file(local_file, "uploaded/test.txt")
        assert success is True
        
        # Download file
        download_path = os.path.join(temp_dir, "downloaded.txt")
        success = provider.download_file("uploaded/test.txt", download_path)
        assert success is True
        
        # Verify content
        with open(download_path, "r") as f:
            assert f.read() == test_content
    
    def test_upload_download_dataframe(self, temp_dir):
        """Test DataFrame upload and download."""
        provider = LocalStorageProvider(temp_dir)
        
        # Create test DataFrame
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        
        # Upload DataFrame as parquet
        success = provider.upload_dataframe(df, "data/test.parquet", format="parquet")
        assert success is True
        
        # Download DataFrame
        downloaded_df = provider.download_dataframe("data/test.parquet", format="parquet")
        pd.testing.assert_frame_equal(df, downloaded_df)
        
        # Test CSV format
        success = provider.upload_dataframe(df, "data/test.csv", format="csv")
        assert success is True
        
        downloaded_csv = provider.download_dataframe("data/test.csv", format="csv")
        pd.testing.assert_frame_equal(df, downloaded_csv)
    
    def test_list_files(self, temp_dir):
        """Test file listing functionality."""
        provider = LocalStorageProvider(temp_dir)
        
        # Create some test files
        test_files = ["file1.txt", "dir/file2.txt", "dir/subdir/file3.txt"]
        for file_path in test_files:
            full_path = provider._get_full_path(file_path)
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text("test")
        
        # List all files
        all_files = provider.list_files()
        assert len(all_files) == 3
        assert "file1.txt" in all_files
        assert "dir/file2.txt" in all_files
        assert "dir/subdir/file3.txt" in all_files
        
        # List with prefix
        dir_files = provider.list_files("dir/")
        assert len(dir_files) == 2
        assert all(f.startswith("dir/") for f in dir_files)
    
    def test_file_operations(self, temp_dir):
        """Test file existence, deletion, and size operations."""
        provider = LocalStorageProvider(temp_dir)
        
        # Create a test file
        test_content = "test content for size check"
        df = pd.DataFrame({"data": [test_content]})
        provider.upload_dataframe(df, "test_file.parquet")
        
        # Test file existence
        assert provider.file_exists("test_file.parquet") is True
        assert provider.file_exists("nonexistent.txt") is False
        
        # Test file size
        size = provider.get_file_size("test_file.parquet")
        assert size > 0
        assert provider.get_file_size("nonexistent.txt") == 0
        
        # Test file deletion
        success = provider.delete_file("test_file.parquet")
        assert success is True
        assert provider.file_exists("test_file.parquet") is False
        
        # Test deleting non-existent file
        success = provider.delete_file("nonexistent.txt")
        assert success is False


class TestLocalDatabaseProvider:
    """Test LocalDatabaseProvider functionality."""
    
    def test_initialization(self, temp_dir):
        """Test database provider initialization."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        assert provider.db_path == db_path
        assert provider.connection is None
    
    def test_connection(self, temp_dir):
        """Test database connection."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        conn = provider.connect()
        assert conn is not None
        assert provider.connection is not None
        
        # Test that subsequent calls return same connection
        conn2 = provider.connect()
        assert conn is conn2
    
    def test_create_table(self, temp_dir):
        """Test table creation."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        schema = {
            "id": "INTEGER PRIMARY KEY",
            "name": "TEXT NOT NULL",
            "value": "REAL"
        }
        
        success = provider.create_table("test_table", schema)
        assert success is True
        
        # Verify table exists
        assert provider.table_exists("test_table") is True
        assert provider.table_exists("nonexistent_table") is False
    
    def test_dataframe_operations(self, temp_dir):
        """Test DataFrame insert and fetch operations."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        # Create test table
        schema = {"id": "INTEGER", "name": "TEXT", "value": "REAL"}
        provider.create_table("test_data", schema)
        
        # Create and insert test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 15.7]
        })
        
        success = provider.insert_dataframe(df, "test_data")
        assert success is True
        
        # Fetch data back
        query = "SELECT * FROM test_data ORDER BY id"
        fetched_df = provider.fetch_dataframe(query)
        
        assert len(fetched_df) == 3
        assert list(fetched_df.columns) == ["id", "name", "value"]
        assert fetched_df.iloc[0]["name"] == "Alice"
    
    def test_get_table_schema(self, temp_dir):
        """Test getting table schema."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        # Create test table
        schema = {
            "id": "INTEGER PRIMARY KEY",
            "name": "TEXT NOT NULL",
            "value": "REAL"
        }
        provider.create_table("test_table", schema)
        
        # Get schema
        fetched_schema = provider.get_table_schema("test_table")
        
        assert "id" in fetched_schema
        assert "name" in fetched_schema
        assert "value" in fetched_schema
    
    def test_close_connection(self, temp_dir):
        """Test closing database connection."""
        db_path = os.path.join(temp_dir, "test.db")
        provider = LocalDatabaseProvider(db_path)
        
        # Connect and then close
        provider.connect()
        assert provider.connection is not None
        
        provider.close()
        assert provider.connection is None


class TestLocalCloudProvider:
    """Test LocalCloudProvider integration."""
    
    def test_initialization(self, temp_dir):
        """Test cloud provider initialization."""
        config = {"base_path": temp_dir}
        provider = LocalCloudProvider(config)
        
        assert provider.base_path == temp_dir
        assert provider._storage_provider is None
        assert provider._database_provider is None
    
    def test_get_providers(self, temp_dir):
        """Test getting provider instances."""
        config = {"base_path": temp_dir}
        provider = LocalCloudProvider(config)
        
        # Test storage provider
        storage = provider.get_storage_provider()
        assert isinstance(storage, LocalStorageProvider)
        
        # Test database provider
        database = provider.get_database_provider()
        assert isinstance(database, LocalDatabaseProvider)
        
        # Test compute provider (not implemented locally)
        compute = provider.get_compute_provider()
        assert compute is not None
        
        # Test that subsequent calls return same instances
        assert provider.get_storage_provider() is storage
        assert provider.get_database_provider() is database
    
    def test_health_check(self, temp_dir):
        """Test health check functionality."""
        config = {"base_path": temp_dir}
        provider = LocalCloudProvider(config)
        
        health = provider.health_check()
        
        assert "storage" in health
        assert "database" in health
        assert "compute" in health
        
        # Storage should be healthy
        assert health["storage"] is True
        
        # Database should be healthy
        assert health["database"] is True
        
        # Compute is not implemented locally
        assert health["compute"] is False


@pytest.mark.aws
class TestAWSProviderImport:
    """Test AWS provider import and initialization."""
    
    def test_aws_provider_import_without_dependencies(self):
        """Test AWS provider import when dependencies not available."""
        with patch.dict('sys.modules', {'boto3': None, 'psycopg2': None}):
            with pytest.raises(ImportError, match="AWS dependencies not installed"):
                CloudProviderFactory.create("aws", {
                    "storage": {"bucket_name": "test"},
                    "database": {"endpoint": "test"}
                })


# Integration-style tests that could be moved to integration test suite

class TestProviderIntegration:
    """Test provider integration scenarios."""
    
    def test_end_to_end_data_flow(self, temp_dir):
        """Test complete data flow through local provider."""
        config = {"base_path": temp_dir}
        provider = LocalCloudProvider(config)
        
        storage = provider.get_storage_provider()
        database = provider.get_database_provider()
        
        # Create test data
        test_df = pd.DataFrame({
            "session_key": [1, 2],
            "driver_number": [1, 44],
            "lap_time": [90.5, 91.2]
        })
        
        # Store in storage
        storage.upload_dataframe(test_df, "raw/laps.parquet")
        
        # Create database table
        schema = {
            "session_key": "INTEGER",
            "driver_number": "INTEGER", 
            "lap_time": "REAL"
        }
        database.create_table("laps", schema)
        
        # Load from storage and save to database
        loaded_df = storage.download_dataframe("raw/laps.parquet")
        database.insert_dataframe(loaded_df, "laps")
        
        # Fetch from database
        result_df = database.fetch_dataframe("SELECT * FROM laps ORDER BY session_key")
        
        # Verify data integrity
        assert len(result_df) == 2
        assert result_df.iloc[0]["driver_number"] == 1
        assert result_df.iloc[1]["driver_number"] == 44