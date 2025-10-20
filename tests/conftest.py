"""Test configuration and fixtures."""

import pytest
import tempfile
import os
import shutil
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from f1_data_platform.config.settings import Settings, StorageConfig, DatabaseConfig
from f1_data_platform.cloud_swap import CloudProviderFactory
from f1_data_platform.extractors import OpenF1Client


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def local_settings(temp_dir):
    """Create test settings for local environment."""
    return Settings(
        environment="local",
        storage=StorageConfig(
            provider="local",
            local_path=temp_dir
        ),
        database=DatabaseConfig(
            provider="local",
            db_path=os.path.join(temp_dir, "test.db")
        )
    )


@pytest.fixture
def mock_cloud_provider(local_settings):
    """Create a mock cloud provider for testing."""
    return CloudProviderFactory.create("local", local_settings.get_cloud_provider_config())


@pytest.fixture
def sample_meetings_data():
    """Sample meetings data for testing."""
    return [
        {
            "meeting_key": 1219,
            "meeting_name": "Singapore Grand Prix",
            "meeting_official_name": "FORMULA 1 SINGAPORE AIRLINES SINGAPORE GRAND PRIX 2023",
            "location": "Marina Bay",
            "country_name": "Singapore",
            "country_code": "SGP",
            "country_key": 157,
            "circuit_key": 61,
            "circuit_short_name": "Singapore",
            "date_start": "2023-09-15T09:30:00+00:00",
            "gmt_offset": "08:00:00",
            "year": 2023
        }
    ]


@pytest.fixture
def sample_sessions_data():
    """Sample sessions data for testing."""
    return [
        {
            "session_key": 9158,
            "session_name": "Practice 1",
            "session_type": "Practice",
            "meeting_key": 1219,
            "location": "Marina Bay",
            "country_name": "Singapore",
            "country_code": "SGP",
            "country_key": 157,
            "circuit_key": 61,
            "circuit_short_name": "Singapore",
            "date_start": "2023-09-15T09:30:00+00:00",
            "date_end": "2023-09-15T11:00:00+00:00",
            "gmt_offset": "08:00:00",
            "year": 2023
        },
        {
            "session_key": 9165,
            "session_name": "Race",
            "session_type": "Race",
            "meeting_key": 1219,
            "location": "Marina Bay",
            "country_name": "Singapore",
            "country_code": "SGP",
            "country_key": 157,
            "circuit_key": 61,
            "circuit_short_name": "Singapore",
            "date_start": "2023-09-17T13:00:00+00:00",
            "date_end": "2023-09-17T15:00:00+00:00",
            "gmt_offset": "08:00:00",
            "year": 2023
        }
    ]


@pytest.fixture
def sample_drivers_data():
    """Sample drivers data for testing."""
    return [
        {
            "session_key": 9158,
            "meeting_key": 1219,
            "driver_number": 1,
            "broadcast_name": "M VERSTAPPEN",
            "country_code": "NED",
            "first_name": "Max",
            "full_name": "Max VERSTAPPEN",
            "headshot_url": "https://example.com/verstappen.png",
            "last_name": "Verstappen",
            "name_acronym": "VER",
            "team_colour": "3671C6",
            "team_name": "Red Bull Racing",
            "year": 2023
        },
        {
            "session_key": 9158,
            "meeting_key": 1219,
            "driver_number": 44,
            "broadcast_name": "L HAMILTON",
            "country_code": "GBR",
            "first_name": "Lewis",
            "full_name": "Lewis HAMILTON",
            "headshot_url": "https://example.com/hamilton.png",
            "last_name": "Hamilton",
            "name_acronym": "HAM",
            "team_colour": "00D2BE",
            "team_name": "Mercedes",
            "year": 2023
        }
    ]


@pytest.fixture
def sample_laps_data():
    """Sample laps data for testing."""
    return [
        {
            "session_key": 9165,
            "meeting_key": 1219,
            "driver_number": 1,
            "date_start": "2023-09-17T13:00:30+00:00",
            "duration_sector_1": 26.5,
            "duration_sector_2": 38.2,
            "duration_sector_3": 26.8,
            "i1_speed": 307,
            "i2_speed": 277,
            "is_pit_out_lap": False,
            "lap_duration": 91.5,
            "lap_number": 1,
            "st_speed": 298,
            "year": 2023
        },
        {
            "session_key": 9165,
            "meeting_key": 1219,
            "driver_number": 44,
            "date_start": "2023-09-17T13:00:32+00:00",
            "duration_sector_1": 26.8,
            "duration_sector_2": 38.5,
            "duration_sector_3": 27.1,
            "i1_speed": 305,
            "i2_speed": 275,
            "is_pit_out_lap": False,
            "lap_duration": 92.4,
            "lap_number": 1,
            "st_speed": 296,
            "year": 2023
        }
    ]


@pytest.fixture
def mock_openf1_client():
    """Create a mock OpenF1 client for testing."""
    client = MagicMock(spec=OpenF1Client)
    
    # Configure default return values
    client.health_check.return_value = {
        "status": "healthy",
        "api_accessible": True,
        "sample_data_available": True,
        "response_time_ms": 100
    }
    
    return client


@pytest.fixture
def mock_requests_response():
    """Create a mock requests response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    return mock_response


class MockDatabase:
    """Mock database provider for testing."""
    
    def __init__(self):
        self.tables = {}
        self.connected = True
    
    def connect(self):
        return self
    
    def execute_query(self, query, params=None):
        return MagicMock()
    
    def execute_many(self, query, params):
        return MagicMock()
    
    def fetch_dataframe(self, query, params=None):
        return pd.DataFrame()
    
    def insert_dataframe(self, df, table_name, if_exists="append"):
        self.tables[table_name] = df
        return True
    
    def create_table(self, table_name, schema):
        self.tables[table_name] = pd.DataFrame()
        return True
    
    def table_exists(self, table_name):
        return table_name in self.tables
    
    def get_table_schema(self, table_name):
        return {}
    
    def close(self):
        self.connected = False


class MockStorage:
    """Mock storage provider for testing."""
    
    def __init__(self):
        self.files = {}
    
    def upload_file(self, local_path, remote_path):
        self.files[remote_path] = local_path
        return True
    
    def download_file(self, remote_path, local_path):
        return remote_path in self.files
    
    def upload_dataframe(self, df, remote_path, format="parquet"):
        self.files[remote_path] = df
        return True
    
    def download_dataframe(self, remote_path, format="parquet"):
        if remote_path in self.files and isinstance(self.files[remote_path], pd.DataFrame):
            return self.files[remote_path]
        return pd.DataFrame()
    
    def list_files(self, prefix=""):
        return [f for f in self.files.keys() if f.startswith(prefix)]
    
    def delete_file(self, remote_path):
        if remote_path in self.files:
            del self.files[remote_path]
            return True
        return False
    
    def file_exists(self, remote_path):
        return remote_path in self.files
    
    def get_file_size(self, remote_path):
        return 1000 if remote_path in self.files else 0


@pytest.fixture
def mock_database():
    """Provide a mock database for testing."""
    return MockDatabase()


@pytest.fixture
def mock_storage():
    """Provide a mock storage for testing."""
    return MockStorage()


# AWS Mocking fixtures

@pytest.fixture
def mock_boto3_client():
    """Mock boto3 client for AWS testing."""
    with patch('boto3.client') as mock_client:
        mock_s3 = MagicMock()
        mock_s3.upload_file.return_value = None
        mock_s3.download_file.return_value = None
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_s3.head_object.return_value = {"ContentLength": 1000}
        
        mock_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine for database testing."""
    with patch('sqlalchemy.create_engine') as mock_engine:
        engine = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=MagicMock())
        engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        mock_engine.return_value = engine
        yield engine


# Pytest configuration

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "aws: mark test as AWS-specific")
    config.addinivalue_line("markers", "azure: mark test as Azure-specific")
    config.addinivalue_line("markers", "gcp: mark test as GCP-specific")
    config.addinivalue_line("markers", "slow: mark test as slow running")