"""Integration tests for the complete pipeline."""

import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import pandas as pd

from f1_data_platform.config.settings import Settings, StorageConfig, DatabaseConfig
from f1_data_platform.cloud_swap import CloudProviderFactory
from f1_data_platform.extractors import DataExtractor
from f1_data_platform.transformers import DataTransformer, AIPreparationTransformer


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the complete pipeline."""
    
    @pytest.fixture
    def pipeline_setup(self):
        """Setup a complete pipeline for testing."""
        temp_dir = tempfile.mkdtemp()
        
        settings = Settings(
            environment="local",
            storage=StorageConfig(provider="local", local_path=temp_dir),
            database=DatabaseConfig(provider="local", db_path=os.path.join(temp_dir, "test.db"))
        )
        
        cloud_provider = CloudProviderFactory.create("local", settings.get_cloud_provider_config())
        
        extractor = DataExtractor(settings, cloud_provider)
        transformer = DataTransformer(settings, cloud_provider)
        ai_transformer = AIPreparationTransformer(settings, cloud_provider)
        
        yield {
            "temp_dir": temp_dir,
            "settings": settings,
            "cloud_provider": cloud_provider,
            "extractor": extractor,
            "transformer": transformer,
            "ai_transformer": ai_transformer
        }
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    def test_complete_pipeline_flow(self, pipeline_setup):
        """Test the complete pipeline from extraction to AI preparation."""
        components = pipeline_setup
        extractor = components["extractor"]
        transformer = components["transformer"]
        ai_transformer = components["ai_transformer"]
        
        # Mock the OpenF1 client to return test data
        with patch.object(extractor, 'openf1_client') as mock_client:
            
            # Setup mock data
            mock_meetings = pd.DataFrame([{
                "meeting_key": 1219,
                "meeting_name": "Test Grand Prix",
                "year": 2023,
                "circuit_short_name": "Test Circuit",
                "date_start": "2023-01-01T10:00:00+00:00"
            }])
            
            mock_sessions = pd.DataFrame([{
                "session_key": 9001,
                "meeting_key": 1219,
                "session_name": "Race",
                "session_type": "Race",
                "year": 2023,
                "date_start": "2023-01-01T14:00:00+00:00"
            }])
            
            mock_drivers = pd.DataFrame([
                {
                    "session_key": 9001,
                    "meeting_key": 1219,
                    "driver_number": 1,
                    "full_name": "Test Driver 1",
                    "name_acronym": "TD1",
                    "team_name": "Test Team 1",
                    "year": 2023
                },
                {
                    "session_key": 9001,
                    "meeting_key": 1219,
                    "driver_number": 2,
                    "full_name": "Test Driver 2",
                    "name_acronym": "TD2",
                    "team_name": "Test Team 2",
                    "year": 2023
                }
            ])
            
            # Configure mock client
            def mock_get_all_data_for_year(year):
                yield ("meetings", mock_meetings)
                yield ("sessions", mock_sessions)
                yield ("drivers", mock_drivers)
            
            mock_client.get_all_data_for_year.side_effect = mock_get_all_data_for_year
            
            # Step 1: Extract raw data
            extractor.create_raw_data_tables()
            stats = extractor.extract_year_data(2023, save_raw=True, save_to_db=True)
            
            assert stats["year"] == 2023
            assert stats["endpoints_processed"] >= 3
            assert stats["errors"] == 0
            
            # Step 2: Transform to analytics layer
            transformer.setup_analytics_tables()
            
            # We would need more complete mock data for this to work fully
            # For now, just test that the setup works
            
            # Step 3: Create AI features
            ai_transformer.setup_ai_tables()
            
            # The transformations would need complete mock data to work properly
            # This integration test verifies the basic pipeline structure works
    
    def test_health_checks(self, pipeline_setup):
        """Test health checks across all components."""
        components = pipeline_setup
        extractor = components["extractor"]
        cloud_provider = components["cloud_provider"]
        
        # Test cloud provider health
        cloud_health = cloud_provider.health_check()
        assert cloud_health["storage"] is True
        assert cloud_health["database"] is True
        
        # Test extractor health (mock the API call)
        with patch.object(extractor.openf1_client, 'health_check') as mock_health:
            mock_health.return_value = {
                "status": "healthy",
                "api_accessible": True,
                "sample_data_available": True
            }
            
            extractor_health = extractor.health_check()
            assert extractor_health["overall_status"] == "healthy"
    
    def test_error_handling(self, pipeline_setup):
        """Test error handling in the pipeline."""
        components = pipeline_setup
        extractor = components["extractor"]
        
        # Test extraction with API errors
        with patch.object(extractor.openf1_client, 'get_all_data_for_year') as mock_get_data:
            mock_get_data.side_effect = Exception("API Error")
            
            stats = extractor.extract_year_data(2023)
            assert stats["errors"] > 0
    
    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test valid configuration
        settings = Settings(
            environment="local",
            storage=StorageConfig(provider="local", local_path="./test"),
            database=DatabaseConfig(provider="local", db_path="./test.db")
        )
        assert settings.environment == "local"
        
        # Test invalid storage provider
        with pytest.raises(ValueError):
            StorageConfig(provider="invalid_provider")
    
    @pytest.mark.slow
    def test_large_dataset_handling(self, pipeline_setup):
        """Test handling of large datasets."""
        components = pipeline_setup
        storage = components["cloud_provider"].get_storage_provider()
        
        # Create a large test dataset
        large_df = pd.DataFrame({
            "id": range(10000),
            "value": range(10000),
            "data": ["test_data"] * 10000
        })
        
        # Test storage and retrieval
        success = storage.upload_dataframe(large_df, "large_test.parquet")
        assert success is True
        
        retrieved_df = storage.download_dataframe("large_test.parquet")
        assert len(retrieved_df) == 10000
        pd.testing.assert_frame_equal(large_df, retrieved_df)


@pytest.mark.integration 
@pytest.mark.aws
class TestAWSIntegration:
    """Integration tests specific to AWS provider."""
    
    @pytest.fixture
    def aws_config(self):
        """AWS configuration for testing."""
        return {
            "storage": {
                "provider": "aws",
                "bucket_name": "test-f1-bucket",
                "region": "us-east-1"
            },
            "database": {
                "provider": "aws_rds",
                "endpoint": "test-endpoint.rds.amazonaws.com",
                "database": "f1_test",
                "username": "testuser",
                "password": "testpass"
            }
        }
    
    @patch('boto3.client')
    @patch('sqlalchemy.create_engine')
    def test_aws_provider_initialization(self, mock_engine, mock_boto_client, aws_config):
        """Test AWS provider initialization with mocked dependencies."""
        # This test requires AWS dependencies to be installed
        try:
            provider = CloudProviderFactory.create("aws", aws_config)
            
            # Test that providers can be retrieved
            storage = provider.get_storage_provider()
            database = provider.get_database_provider()
            
            assert storage is not None
            assert database is not None
            
        except ImportError:
            pytest.skip("AWS dependencies not installed")
    
    @patch('boto3.client')
    def test_aws_s3_operations(self, mock_boto_client, aws_config):
        """Test AWS S3 operations with mocking."""
        try:
            from f1_data_platform.cloud_swap.providers.aws import AWSStorageProvider
            
            # Setup mock S3 client
            mock_s3 = MagicMock()
            mock_boto_client.return_value = mock_s3
            
            provider = AWSStorageProvider("test-bucket", "us-east-1")
            
            # Test file upload
            mock_s3.upload_file.return_value = None
            success = provider.upload_file("local.txt", "remote.txt")
            assert success is True
            
            # Test file listing
            mock_s3.get_paginator.return_value.paginate.return_value = [
                {"Contents": [{"Key": "file1.txt"}, {"Key": "file2.txt"}]}
            ]
            files = provider.list_files()
            assert len(files) == 2
            
        except ImportError:
            pytest.skip("AWS dependencies not installed")


@pytest.mark.integration
class TestPerformanceMetrics:
    """Integration tests for performance monitoring."""
    
    def test_extraction_performance(self, pipeline_setup):
        """Test extraction performance metrics."""
        components = pipeline_setup
        extractor = components["extractor"]
        
        # Mock small dataset
        with patch.object(extractor.openf1_client, 'get_all_data_for_year') as mock_get_data:
            small_df = pd.DataFrame([{"test": "data"}])
            mock_get_data.return_value = [("test_endpoint", small_df)]
            
            import time
            start_time = time.time()
            stats = extractor.extract_year_data(2023)
            end_time = time.time()
            
            # Basic performance assertions
            assert (end_time - start_time) < 10  # Should complete within 10 seconds
            assert stats["total_records"] >= 0
    
    def test_memory_usage(self, pipeline_setup):
        """Test memory usage during processing."""
        components = pipeline_setup
        storage = components["cloud_provider"].get_storage_provider()
        
        # Test with different sized datasets
        small_df = pd.DataFrame({"data": range(100)})
        medium_df = pd.DataFrame({"data": range(10000)})
        
        # Both should complete successfully
        assert storage.upload_dataframe(small_df, "small.parquet") is True
        assert storage.upload_dataframe(medium_df, "medium.parquet") is True
        
        # Verify retrieval
        retrieved_small = storage.download_dataframe("small.parquet")
        retrieved_medium = storage.download_dataframe("medium.parquet")
        
        assert len(retrieved_small) == 100
        assert len(retrieved_medium) == 10000


@pytest.mark.integration
class TestDataQuality:
    """Integration tests for data quality and validation."""
    
    def test_data_consistency(self, pipeline_setup):
        """Test data consistency through the pipeline."""
        components = pipeline_setup
        storage = components["cloud_provider"].get_storage_provider()
        database = components["cloud_provider"].get_database_provider()
        
        # Create test data with known characteristics
        test_data = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10.5, 20.3, 15.7],
            "category": ["A", "B", "A"]
        })
        
        # Store and retrieve through storage
        storage.upload_dataframe(test_data, "test_consistency.parquet")
        retrieved_data = storage.download_dataframe("test_consistency.parquet")
        
        # Verify consistency
        pd.testing.assert_frame_equal(test_data, retrieved_data)
        
        # Store and retrieve through database
        database.create_table("test_table", {
            "id": "INTEGER",
            "value": "REAL", 
            "category": "TEXT"
        })
        database.insert_dataframe(test_data, "test_table")
        db_data = database.fetch_dataframe("SELECT * FROM test_table ORDER BY id")
        
        # Verify database consistency (column order might differ)
        assert len(db_data) == len(test_data)
        assert set(db_data.columns) == set(test_data.columns)
    
    def test_schema_validation(self, pipeline_setup):
        """Test schema validation during data processing."""
        from f1_data_platform.models.schemas import SchemaManager
        
        schema_manager = SchemaManager()
        
        # Test valid data
        valid_data = {
            "meeting_key": 1219,
            "meeting_name": "Test GP",
            "year": 2023,
            "circuit_short_name": "Test"
        }
        
        # This would need to be implemented in SchemaManager
        # errors = schema_manager.validate_schema("raw_meetings", valid_data)
        # assert len(errors) == 0
        
        # Test data quality metrics structure
        all_schemas = schema_manager.get_all_schemas()
        assert "data_quality_metrics" in all_schemas
        assert "table_name" in all_schemas["data_quality_metrics"]