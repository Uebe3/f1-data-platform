"""OpenF1 API client for data extraction."""

import time
import requests
from typing import Dict, List, Any, Optional, Generator
from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class APIEndpoint:
    """Configuration for an OpenF1 API endpoint."""
    name: str
    url_path: str
    description: str
    required_params: List[str] = field(default_factory=list)
    optional_params: List[str] = field(default_factory=list)
    batch_size: int = 1000  # Maximum records per request


class OpenF1Client:
    """Client for interacting with the OpenF1 API."""

    # Define all available endpoints
    ENDPOINTS = {
        "meetings": APIEndpoint(
            name="meetings",
            url_path="/meetings",
            description="Grand Prix meetings information",
            optional_params=["year", "country_name", "meeting_key"]
        ),
        "sessions": APIEndpoint(
            name="sessions",
            url_path="/sessions",
            description="Session information (practice, qualifying, race)",
            optional_params=["year", "country_name", "session_name", "session_type", "meeting_key"]
        ),
        "drivers": APIEndpoint(
            name="drivers",
            url_path="/drivers",
            description="Driver information per session",
            optional_params=["driver_number", "session_key", "meeting_key"]
        ),
        "laps": APIEndpoint(
            name="laps",
            url_path="/laps",
            description="Detailed lap information",
            optional_params=["session_key", "driver_number", "lap_number"]
        ),
        "car_data": APIEndpoint(
            name="car_data",
            url_path="/car_data",
            description="Car telemetry data",
            optional_params=["session_key", "driver_number", "date", "speed", "rpm"]
        ),
        "position": APIEndpoint(
            name="position",
            url_path="/position",
            description="Position changes throughout sessions",
            optional_params=["session_key", "driver_number", "meeting_key", "position"]
        ),
        "intervals": APIEndpoint(
            name="intervals",
            url_path="/intervals",
            description="Time intervals between drivers",
            optional_params=["session_key", "driver_number", "interval"]
        ),
        "pit": APIEndpoint(
            name="pit",
            url_path="/pit",
            description="Pit stop information",
            optional_params=["session_key", "driver_number", "pit_duration"]
        ),
        "location": APIEndpoint(
            name="location",
            url_path="/location",
            description="Car location on track",
            optional_params=["session_key", "driver_number", "date"]
        ),
        "stints": APIEndpoint(
            name="stints",
            url_path="/stints",
            description="Tire stint information",
            optional_params=["session_key", "driver_number", "compound"]
        ),
        "weather": APIEndpoint(
            name="weather",
            url_path="/weather",
            description="Weather conditions",
            optional_params=["session_key", "meeting_key"]
        ),
        "race_control": APIEndpoint(
            name="race_control",
            url_path="/race_control",
            description="Race control events and flags",
            optional_params=["session_key", "driver_number", "flag", "category"]
        ),
        "team_radio": APIEndpoint(
            name="team_radio",
            url_path="/team_radio",
            description="Team radio communications",
            optional_params=["session_key", "driver_number"]
        ),
        "overtakes": APIEndpoint(
            name="overtakes",
            url_path="/overtakes",
            description="Overtaking information (beta)",
            optional_params=["session_key", "overtaking_driver_number", "overtaken_driver_number"]
        ),
        "session_result": APIEndpoint(
            name="session_result",
            url_path="/session_result",
            description="Session results (beta)",
            optional_params=["session_key", "position", "driver_number"]
        ),
        "starting_grid": APIEndpoint(
            name="starting_grid",
            url_path="/starting_grid",
            description="Starting grid positions (beta)",
            optional_params=["session_key", "position", "driver_number"]
        )
    }

    def __init__(self, base_url: str = "https://api.openf1.org/v1",
                 rate_limit_delay: float = 0.1, max_retries: int = 3, timeout: int = 30):
        """Initialize the OpenF1 client.
        
        Args:
            base_url: Base URL for the OpenF1 API
            rate_limit_delay: Delay between requests in seconds
            max_retries: Maximum number of retries for failed requests
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            "User-Agent": "F1-Pipeline/1.0.0",
            "Accept": "application/json"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a request to the OpenF1 API with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data
            
        Raises:
            requests.RequestException: If request fails after all retries
        """
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries + 1):
            try:
                # Rate limiting
                if attempt > 0:
                    time.sleep(self.rate_limit_delay * (2 ** attempt))  # Exponential backoff
                else:
                    time.sleep(self.rate_limit_delay)
                
                logger.debug(f"Making request to {url} with params {params} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}")
                if attempt == self.max_retries:
                    logger.error(f"All retries exhausted for {url}")
                    raise
                continue

    def get_data(self, endpoint_name: str, params: Optional[Dict[str, Any]] = None,
                 as_dataframe: bool = True) -> Any:
        """Get data from a specific endpoint.
        
        Args:
            endpoint_name: Name of the endpoint to query
            params: Query parameters
            as_dataframe: Whether to return data as pandas DataFrame
            
        Returns:
            List of records or pandas DataFrame
        """
        if endpoint_name not in self.ENDPOINTS:
            raise ValueError(f"Unknown endpoint: {endpoint_name}. Available: {list(self.ENDPOINTS.keys())}")
        
        endpoint = self.ENDPOINTS[endpoint_name]
        data = self._make_request(endpoint.url_path, params)
        
        if as_dataframe and data:
            df = pd.DataFrame(data)
            # Add metadata columns
            df["_extracted_at"] = datetime.utcnow()
            df["_endpoint"] = endpoint_name
            return df
        
        return data

    def get_meetings(self, year: Optional[int] = None, country_name: Optional[str] = None) -> pd.DataFrame:
        """Get meeting data for specified year or country."""
        params = {}
        if year:
            params["year"] = year
        if country_name:
            params["country_name"] = country_name
        
        return self.get_data("meetings", params)

    def get_sessions(self, year: Optional[int] = None, meeting_key: Optional[int] = None,
                    session_type: Optional[str] = None) -> pd.DataFrame:
        """Get session data."""
        params = {}
        if year:
            params["year"] = year
        if meeting_key:
            params["meeting_key"] = meeting_key
        if session_type:
            params["session_type"] = session_type
        
        return self.get_data("sessions", params)

    def get_drivers(self, session_key: int) -> pd.DataFrame:
        """Get driver data for a specific session."""
        return self.get_data("drivers", {"session_key": session_key})

    def get_laps(self, session_key: int, driver_number: Optional[int] = None) -> pd.DataFrame:
        """Get lap data for a specific session."""
        params = {"session_key": session_key}
        if driver_number:
            params["driver_number"] = driver_number
        
        return self.get_data("laps", params)

    def get_all_data_for_year(self, year: int) -> Generator[tuple, None, None]:
        """Get all available data for a specific year.
        
        Yields:
            Tuple of (endpoint_name, dataframe) for each endpoint
        """
        logger.info(f"Starting data extraction for year {year}")
        
        # First, get meetings for the year
        meetings_df = self.get_meetings(year=year)
        yield ("meetings", meetings_df)
        
        if meetings_df.empty:
            logger.warning(f"No meetings found for year {year}")
            return
        
        # Get sessions for each meeting
        all_sessions = []
        for _, meeting in meetings_df.iterrows():
            sessions_df = self.get_sessions(meeting_key=meeting["meeting_key"])
            all_sessions.append(sessions_df)
        
        if all_sessions:
            sessions_df = pd.concat(all_sessions, ignore_index=True)
            yield ("sessions", sessions_df)
        else:
            logger.warning(f"No sessions found for year {year}")
            return
        
        # For each session, get detailed data
        for _, session in sessions_df.iterrows():
            session_key = session["session_key"]
            session_name = session.get("session_name", "Unknown")
            meeting_name = session.get("meeting_key", "Unknown")
            
            logger.info(f"Extracting data for {meeting_name} - {session_name} (session_key: {session_key})")
            
            # Get data for each endpoint that uses session_key
            session_endpoints = ["drivers", "laps", "position", "intervals", "pit", 
                               "location", "stints", "weather", "race_control", 
                               "team_radio", "overtakes", "session_result", "starting_grid"]
            
            for endpoint_name in session_endpoints:
                try:
                    data_df = self.get_data(endpoint_name, {"session_key": session_key})
                    if not data_df.empty:
                        # Add session context
                        data_df["session_key"] = session_key
                        data_df["meeting_key"] = session.get("meeting_key")
                        yield (endpoint_name, data_df)
                    else:
                        logger.debug(f"No data found for {endpoint_name} in session {session_key}")
                        
                except Exception as e:
                    logger.error(f"Error extracting {endpoint_name} for session {session_key}: {e}")
                    continue
            
            # Get car data (this endpoint typically has a lot of data)
            try:
                car_data_df = self.get_data("car_data", {"session_key": session_key})
                if not car_data_df.empty:
                    car_data_df["session_key"] = session_key
                    car_data_df["meeting_key"] = session.get("meeting_key")
                    yield ("car_data", car_data_df)
            except Exception as e:
                logger.error(f"Error extracting car_data for session {session_key}: {e}")

    def get_all_data_for_years(self, years: List[int]) -> Generator[tuple, None, None]:
        """Get all available data for multiple years.
        
        Args:
            years: List of years to extract data for
            
        Yields:
            Tuple of (year, endpoint_name, dataframe) for each endpoint
        """
        for year in years:
            logger.info(f"Processing year {year}")
            for endpoint_name, dataframe in self.get_all_data_for_year(year):
                # Add year information
                dataframe["year"] = year
                yield (year, endpoint_name, dataframe)

    def health_check(self) -> Dict[str, Any]:
        """Check API health and connectivity."""
        try:
            # Try to get recent meetings
            response = self._make_request("/meetings", {"year": 2023})
            return {
                "status": "healthy",
                "api_accessible": True,
                "sample_data_available": len(response) > 0,
                "response_time_ms": self.rate_limit_delay * 1000
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "api_accessible": False,
                "error": str(e),
                "response_time_ms": None
            }