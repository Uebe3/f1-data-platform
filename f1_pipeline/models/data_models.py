"""Pydantic models for F1 pipeline data structures."""

from datetime import datetime
from typing import Optional, List, Any, Union
from pydantic import BaseModel, Field, validator
import pandas as pd


# Raw Data Models (mapping to OpenF1 API responses)

class RawMeeting(BaseModel):
    """Raw meeting data from OpenF1 API."""
    meeting_key: int
    meeting_name: str
    meeting_official_name: Optional[str] = None
    location: str
    country_name: str
    country_code: str
    country_key: int
    circuit_key: int
    circuit_short_name: str
    date_start: datetime
    gmt_offset: str
    year: int
    _extracted_at: datetime = Field(default_factory=datetime.utcnow)
    _endpoint: str = "meetings"


class RawSession(BaseModel):
    """Raw session data from OpenF1 API."""
    session_key: int
    session_name: str
    session_type: str
    meeting_key: int
    location: str
    country_name: str
    country_code: str
    country_key: int
    circuit_key: int
    circuit_short_name: str
    date_start: datetime
    date_end: Optional[datetime] = None
    gmt_offset: str
    year: int
    _extracted_at: datetime = Field(default_factory=datetime.utcnow)
    _endpoint: str = "sessions"


class RawDriver(BaseModel):
    """Raw driver data from OpenF1 API."""
    session_key: int
    meeting_key: int
    driver_number: int
    broadcast_name: str
    country_code: str
    first_name: str
    full_name: str
    headshot_url: Optional[str] = None
    last_name: str
    name_acronym: str
    team_colour: str
    team_name: str
    year: int
    _extracted_at: datetime = Field(default_factory=datetime.utcnow)
    _endpoint: str = "drivers"


class RawLap(BaseModel):
    """Raw lap data from OpenF1 API."""
    session_key: int
    meeting_key: int
    driver_number: int
    date_start: datetime
    duration_sector_1: Optional[float] = None
    duration_sector_2: Optional[float] = None
    duration_sector_3: Optional[float] = None
    i1_speed: Optional[int] = None
    i2_speed: Optional[int] = None
    is_pit_out_lap: Optional[bool] = None
    lap_duration: Optional[float] = None
    lap_number: int
    st_speed: Optional[int] = None
    year: int
    _extracted_at: datetime = Field(default_factory=datetime.utcnow)
    _endpoint: str = "laps"


class RawCarData(BaseModel):
    """Raw car telemetry data from OpenF1 API."""
    session_key: int
    meeting_key: int
    driver_number: int
    date: datetime
    brake: Optional[int] = None
    drs: Optional[int] = None
    n_gear: Optional[int] = None
    rpm: Optional[int] = None
    speed: Optional[int] = None
    throttle: Optional[int] = None
    year: int
    _extracted_at: datetime = Field(default_factory=datetime.utcnow)
    _endpoint: str = "car_data"


# Analytics Layer Models

class GrandPrixResult(BaseModel):
    """Grand Prix results with championship context."""
    result_id: str = Field(..., description="Unique identifier for the result")
    date: datetime = Field(..., description="Race date")
    year: int = Field(..., description="Season year")
    grand_prix: str = Field(..., description="Grand Prix name")
    circuit_name: str = Field(..., description="Circuit name")
    driver_number: int = Field(..., description="Driver number")
    driver_name: str = Field(..., description="Driver full name")
    driver_acronym: str = Field(..., description="Driver 3-letter acronym")
    team_name: str = Field(..., description="Team name")
    
    # Race performance metrics
    starting_grid_position: Optional[int] = Field(None, description="Starting grid position")
    final_position: Optional[int] = Field(None, description="Final race position")
    points: float = Field(0.0, description="Points earned in this race")
    fastest_lap: Optional[float] = Field(None, description="Fastest lap time in seconds")
    total_time_penalty: float = Field(0.0, description="Total time penalty in seconds")
    
    # Championship context
    total_season_points: float = Field(0.0, description="Total points in season up to this race")
    drivers_championship_ranking: Optional[int] = Field(None, description="Position in drivers championship")
    points_from_first: float = Field(0.0, description="Points behind championship leader")
    
    # Race metadata
    dnf: bool = Field(False, description="Did not finish")
    dns: bool = Field(False, description="Did not start")
    dsq: bool = Field(False, description="Disqualified")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class GrandPrixPerformance(BaseModel):
    """Grand Prix performance metrics."""
    performance_id: str = Field(..., description="Unique identifier for the performance record")
    date: datetime = Field(..., description="Session date")
    year: int = Field(..., description="Season year")
    grand_prix: str = Field(..., description="Grand Prix name")
    session_name: str = Field(..., description="Session name (Practice 1, Qualifying, Race, etc.)")
    driver_number: int = Field(..., description="Driver number")
    driver_name: str = Field(..., description="Driver full name")
    
    # Performance metrics
    best_lap_time: Optional[float] = Field(None, description="Best lap time in seconds")
    average_lap_time: Optional[float] = Field(None, description="Average lap time in seconds")
    total_pit_time: float = Field(0.0, description="Total time spent in pits (seconds)")
    pit_stops: int = Field(0, description="Number of pit stops")
    
    # Telemetry aggregates
    avg_speed: Optional[float] = Field(None, description="Average speed (km/h)")
    max_speed: Optional[float] = Field(None, description="Maximum speed (km/h)")
    avg_throttle: Optional[float] = Field(None, description="Average throttle percentage")
    time_spent_braking_pct: Optional[float] = Field(None, description="Percentage of time spent braking")
    
    # Tire information
    tire_compounds_used: Optional[List[str]] = Field(None, description="Tire compounds used in session")
    tire_stint_count: int = Field(0, description="Number of tire stints")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DriverChampionshipStanding(BaseModel):
    """Driver championship standings over time."""
    standing_id: str = Field(..., description="Unique identifier for the standing")
    year: int = Field(..., description="Championship year")
    race_round: int = Field(..., description="Race round number in season")
    after_race: str = Field(..., description="Grand Prix name after which this standing applies")
    driver_number: int = Field(..., description="Driver number")
    driver_name: str = Field(..., description="Driver full name")
    team_name: str = Field(..., description="Team name")
    
    # Championship metrics
    position: int = Field(..., description="Championship position")
    points: float = Field(..., description="Total championship points")
    points_behind_leader: float = Field(0.0, description="Points behind championship leader")
    points_ahead_next: float = Field(0.0, description="Points ahead of next driver")
    
    # Race results in this standing
    wins: int = Field(0, description="Number of wins")
    podiums: int = Field(0, description="Number of podium finishes")
    points_finishes: int = Field(0, description="Number of points-scoring finishes")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


# AI Preparation Layer Models

class DriverPerformanceFeatures(BaseModel):
    """Feature-engineered dataset for AI models."""
    feature_id: str = Field(..., description="Unique identifier for feature record")
    year: int = Field(..., description="Season year")
    race_round: int = Field(..., description="Race round number")
    session_type: str = Field(..., description="Session type")
    driver_number: int = Field(..., description="Driver number")
    
    # Derived performance features
    pace_vs_teammate: Optional[float] = Field(None, description="Lap time delta vs teammate")
    pace_vs_field: Optional[float] = Field(None, description="Lap time delta vs field average")
    consistency_score: Optional[float] = Field(None, description="Lap time consistency (lower = more consistent)")
    
    # Contextual features
    track_position_start: Optional[int] = Field(None, description="Starting track position")
    track_position_avg: Optional[float] = Field(None, description="Average track position during session")
    
    # Car performance proxies
    straight_line_speed_rank: Optional[int] = Field(None, description="Rank in straight line speed")
    cornering_speed_rank: Optional[int] = Field(None, description="Rank in cornering speed")
    
    # Environmental features
    air_temperature: Optional[float] = Field(None, description="Air temperature (°C)")
    track_temperature: Optional[float] = Field(None, description="Track temperature (°C)")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class RaceContextFeatures(BaseModel):
    """Race context features for AI models."""
    context_id: str = Field(..., description="Unique identifier for context record")
    year: int = Field(..., description="Season year")
    race_round: int = Field(..., description="Race round number")
    grand_prix: str = Field(..., description="Grand Prix name")
    
    # Track characteristics
    circuit_length: Optional[float] = Field(None, description="Circuit length (km)")
    number_of_turns: Optional[int] = Field(None, description="Number of turns")
    overtaking_difficulty: Optional[float] = Field(None, description="Overtaking difficulty score (derived)")
    
    # Race characteristics
    safety_car_deployments: int = Field(0, description="Number of safety car deployments")
    red_flag_periods: int = Field(0, description="Number of red flag periods")
    total_race_time: Optional[float] = Field(None, description="Total race time (seconds)")
    
    # Weather features
    weather_changes: bool = Field(False, description="Weather conditions changed during session")
    rain_probability: Optional[float] = Field(None, description="Probability of rain during session")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


# Metadata Models

class SchemaMetadata(BaseModel):
    """Metadata for database schemas and tables."""
    table_name: str = Field(..., description="Name of the database table")
    schema_version: str = Field(..., description="Version of the schema")
    description: str = Field(..., description="Description of the table purpose")
    data_source: str = Field(..., description="Source of the data (e.g., 'openf1_api')")
    
    # Field metadata
    field_descriptions: dict = Field(..., description="Mapping of field names to descriptions")
    field_types: dict = Field(..., description="Mapping of field names to data types")
    
    # Lineage information
    upstream_dependencies: List[str] = Field(default_factory=list, description="Tables this depends on")
    downstream_dependencies: List[str] = Field(default_factory=list, description="Tables that depend on this")
    
    # Update information
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    update_frequency: Optional[str] = Field(None, description="How often this table is updated")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DataQualityMetrics(BaseModel):
    """Data quality metrics for monitoring."""
    table_name: str = Field(..., description="Name of the table being measured")
    measurement_date: datetime = Field(default_factory=datetime.utcnow)
    
    # Completeness metrics
    total_records: int = Field(..., description="Total number of records")
    null_counts: dict = Field(..., description="Count of null values per field")
    completeness_score: float = Field(..., description="Overall completeness score (0-1)")
    
    # Consistency metrics
    duplicate_records: int = Field(0, description="Number of duplicate records")
    constraint_violations: int = Field(0, description="Number of constraint violations")
    
    # Timeliness metrics
    latest_data_timestamp: Optional[datetime] = Field(None, description="Timestamp of most recent data")
    data_freshness_hours: Optional[float] = Field(None, description="Hours since last data update")
    
    # Accuracy indicators
    outlier_count: int = Field(0, description="Number of statistical outliers detected")
    anomaly_score: Optional[float] = Field(None, description="Overall anomaly score")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)