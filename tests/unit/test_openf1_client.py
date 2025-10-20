"""Unit tests for OpenF1 client."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import requests

from f1_data_platform.extractors.openf1_client import OpenF1Client, APIEndpoint


class TestAPIEndpoint:
    """Test APIEndpoint dataclass."""
    
    def test_api_endpoint_creation(self):
        """Test creating an API endpoint."""
        endpoint = APIEndpoint(
            name="test_endpoint",
            url_path="/test",
            description="Test endpoint",
            required_params=["param1"],
            optional_params=["param2"]
        )
        
        assert endpoint.name == "test_endpoint"
        assert endpoint.url_path == "/test"
        assert endpoint.description == "Test endpoint"
        assert endpoint.required_params == ["param1"]
        assert endpoint.optional_params == ["param2"]
        assert endpoint.batch_size == 1000  # Default value


class TestOpenF1Client:
    """Test OpenF1Client functionality."""
    
    def test_client_initialization(self):
        """Test client initialization with default parameters."""
        client = OpenF1Client()
        
        assert client.base_url == "https://api.openf1.org/v1"
        assert client.rate_limit_delay == 0.1
        assert client.max_retries == 3
        assert client.timeout == 30
        assert "User-Agent" in client.session.headers
    
    def test_client_initialization_custom_params(self):
        """Test client initialization with custom parameters."""
        client = OpenF1Client(
            base_url="https://custom.api.com/v2",
            rate_limit_delay=0.5,
            max_retries=5,
            timeout=60
        )
        
        assert client.base_url == "https://custom.api.com/v2"
        assert client.rate_limit_delay == 0.5
        assert client.max_retries == 5
        assert client.timeout == 60
    
    @patch('f1_pipeline.extractors.openf1_client.time.sleep')
    @patch('requests.Session.get')
    def test_make_request_success(self, mock_get, mock_sleep):
        """Test successful API request."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        client = OpenF1Client()
        result = client._make_request("/test", {"param": "value"})
        
        assert result == {"data": "test"}
        mock_get.assert_called_once()
        mock_sleep.assert_called_once_with(0.1)
    
    @patch('f1_pipeline.extractors.openf1_client.time.sleep')
    @patch('requests.Session.get')
    def test_make_request_retry_on_failure(self, mock_get, mock_sleep):
        """Test request retry mechanism."""
        # Setup mock to fail twice then succeed
        mock_response_fail = MagicMock()
        mock_response_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        
        mock_response_success = MagicMock()
        mock_response_success.json.return_value = {"data": "success"}
        mock_response_success.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response_fail, mock_response_fail, mock_response_success]
        
        client = OpenF1Client(max_retries=3)
        result = client._make_request("/test")
        
        assert result == {"data": "success"}
        assert mock_get.call_count == 3
        # Should sleep with exponential backoff
        assert mock_sleep.call_count == 3
    
    @patch('requests.Session.get')
    def test_make_request_exhausted_retries(self, mock_get):
        """Test request failure after all retries exhausted."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_get.return_value = mock_response
        
        client = OpenF1Client(max_retries=2)
        
        with pytest.raises(requests.exceptions.HTTPError):
            client._make_request("/test")
        
        assert mock_get.call_count == 3  # Initial + 2 retries
    
    @patch.object(OpenF1Client, '_make_request')
    def test_get_data_as_dataframe(self, mock_make_request):
        """Test getting data as DataFrame."""
        mock_make_request.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        
        client = OpenF1Client()
        result = client.get_data("meetings", as_dataframe=True)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns
        assert "_extracted_at" in result.columns
        assert "_endpoint" in result.columns
        assert result.iloc[0]["_endpoint"] == "meetings"
    
    @patch.object(OpenF1Client, '_make_request')
    def test_get_data_as_list(self, mock_make_request):
        """Test getting data as list."""
        mock_data = [{"id": 1, "name": "test1"}]
        mock_make_request.return_value = mock_data
        
        client = OpenF1Client()
        result = client.get_data("meetings", as_dataframe=False)
        
        assert result == mock_data
    
    def test_get_data_invalid_endpoint(self):
        """Test error for invalid endpoint."""
        client = OpenF1Client()
        
        with pytest.raises(ValueError, match="Unknown endpoint: invalid_endpoint"):
            client.get_data("invalid_endpoint")
    
    @patch.object(OpenF1Client, 'get_data')
    def test_get_meetings(self, mock_get_data):
        """Test get_meetings method."""
        mock_df = pd.DataFrame([{"meeting_key": 1, "year": 2023}])
        mock_get_data.return_value = mock_df
        
        client = OpenF1Client()
        result = client.get_meetings(year=2023, country_name="Monaco")
        
        mock_get_data.assert_called_once_with("meetings", {"year": 2023, "country_name": "Monaco"})
        assert result.equals(mock_df)
    
    @patch.object(OpenF1Client, 'get_data')
    def test_get_sessions(self, mock_get_data):
        """Test get_sessions method."""
        mock_df = pd.DataFrame([{"session_key": 1, "year": 2023}])
        mock_get_data.return_value = mock_df
        
        client = OpenF1Client()
        result = client.get_sessions(year=2023, meeting_key=1219, session_type="Race")
        
        expected_params = {"year": 2023, "meeting_key": 1219, "session_type": "Race"}
        mock_get_data.assert_called_once_with("sessions", expected_params)
        assert result.equals(mock_df)
    
    @patch.object(OpenF1Client, 'get_data')
    def test_get_drivers(self, mock_get_data):
        """Test get_drivers method."""
        mock_df = pd.DataFrame([{"driver_number": 1, "name": "Test Driver"}])
        mock_get_data.return_value = mock_df
        
        client = OpenF1Client()
        result = client.get_drivers(session_key=9158)
        
        mock_get_data.assert_called_once_with("drivers", {"session_key": 9158})
        assert result.equals(mock_df)
    
    @patch.object(OpenF1Client, 'get_data')
    def test_get_laps(self, mock_get_data):
        """Test get_laps method."""
        mock_df = pd.DataFrame([{"lap_number": 1, "lap_duration": 90.5}])
        mock_get_data.return_value = mock_df
        
        client = OpenF1Client()
        result = client.get_laps(session_key=9158, driver_number=1)
        
        expected_params = {"session_key": 9158, "driver_number": 1}
        mock_get_data.assert_called_once_with("laps", expected_params)
        assert result.equals(mock_df)
    
    @patch.object(OpenF1Client, 'get_meetings')
    @patch.object(OpenF1Client, 'get_sessions')
    @patch.object(OpenF1Client, 'get_data')
    def test_get_all_data_for_year(self, mock_get_data, mock_get_sessions, mock_get_meetings):
        """Test getting all data for a year."""
        # Setup mock data
        meetings_df = pd.DataFrame([{"meeting_key": 1219, "year": 2023}])
        sessions_df = pd.DataFrame([{
            "session_key": 9158, "meeting_key": 1219, "session_name": "Race"
        }])
        drivers_df = pd.DataFrame([{"driver_number": 1}])
        
        mock_get_meetings.return_value = meetings_df
        mock_get_sessions.return_value = sessions_df
        mock_get_data.return_value = drivers_df
        
        client = OpenF1Client()
        results = list(client.get_all_data_for_year(2023))
        
        # Should yield meetings, sessions, and then session-specific data
        assert len(results) >= 2  # At least meetings and sessions
        assert results[0][0] == "meetings"
        assert results[1][0] == "sessions"
        
        mock_get_meetings.assert_called_once_with(year=2023)
        mock_get_sessions.assert_called()
    
    @patch.object(OpenF1Client, 'get_all_data_for_year')
    def test_get_all_data_for_years(self, mock_get_all_data_for_year):
        """Test getting all data for multiple years."""
        # Mock generator that yields test data
        def mock_generator(year):
            yield ("meetings", pd.DataFrame([{"year": year}]))
        
        mock_get_all_data_for_year.side_effect = mock_generator
        
        client = OpenF1Client()
        results = list(client.get_all_data_for_years([2022, 2023]))
        
        # Should yield data for both years
        assert len(results) == 2
        assert results[0][0] == 2022
        assert results[1][0] == 2023
        
        # Check that get_all_data_for_year was called for each year
        assert mock_get_all_data_for_year.call_count == 2
    
    @patch.object(OpenF1Client, '_make_request')
    def test_health_check_success(self, mock_make_request):
        """Test successful health check."""
        mock_make_request.return_value = [{"meeting_key": 1}]
        
        client = OpenF1Client()
        result = client.health_check()
        
        assert result["status"] == "healthy"
        assert result["api_accessible"] is True
        assert result["sample_data_available"] is True
        assert "response_time_ms" in result
    
    @patch.object(OpenF1Client, '_make_request')
    def test_health_check_failure(self, mock_make_request):
        """Test health check with API failure."""
        mock_make_request.side_effect = requests.exceptions.ConnectionError("API unreachable")
        
        client = OpenF1Client()
        result = client.health_check()
        
        assert result["status"] == "unhealthy"
        assert result["api_accessible"] is False
        assert "error" in result
        assert result["response_time_ms"] is None
    
    def test_endpoints_definition(self):
        """Test that all expected endpoints are defined."""
        client = OpenF1Client()
        
        expected_endpoints = [
            "meetings", "sessions", "drivers", "laps", "car_data",
            "position", "intervals", "pit", "location", "stints",
            "weather", "race_control", "team_radio", "overtakes",
            "session_result", "starting_grid"
        ]
        
        for endpoint in expected_endpoints:
            assert endpoint in client.ENDPOINTS
            assert isinstance(client.ENDPOINTS[endpoint], APIEndpoint)
    
    def test_endpoint_properties(self):
        """Test endpoint properties are properly set."""
        client = OpenF1Client()
        meetings_endpoint = client.ENDPOINTS["meetings"]
        
        assert meetings_endpoint.name == "meetings"
        assert meetings_endpoint.url_path == "/meetings"
        assert "Grand Prix meetings information" in meetings_endpoint.description
        assert isinstance(meetings_endpoint.optional_params, list)
        assert meetings_endpoint.batch_size == 1000