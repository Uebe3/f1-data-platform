"""Database schema definitions and management."""

from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schemas for the F1 pipeline."""
    
    # Raw data table schemas (direct from OpenF1 API)
    RAW_SCHEMAS = {
        "raw_meetings": {
            "meeting_key": "INTEGER PRIMARY KEY",
            "meeting_name": "TEXT NOT NULL",
            "meeting_official_name": "TEXT",
            "location": "TEXT NOT NULL",
            "country_name": "TEXT NOT NULL",
            "country_code": "TEXT NOT NULL",
            "country_key": "INTEGER NOT NULL",
            "circuit_key": "INTEGER NOT NULL",
            "circuit_short_name": "TEXT NOT NULL",
            "date_start": "TIMESTAMP NOT NULL",
            "gmt_offset": "TEXT NOT NULL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL"
        },
        
        "raw_sessions": {
            "session_key": "INTEGER PRIMARY KEY",
            "session_name": "TEXT NOT NULL",
            "session_type": "TEXT NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "location": "TEXT NOT NULL",
            "country_name": "TEXT NOT NULL",
            "country_code": "TEXT NOT NULL",
            "country_key": "INTEGER NOT NULL",
            "circuit_key": "INTEGER NOT NULL",
            "circuit_short_name": "TEXT NOT NULL",
            "date_start": "TIMESTAMP NOT NULL",
            "date_end": "TIMESTAMP",
            "gmt_offset": "TEXT NOT NULL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_drivers": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "broadcast_name": "TEXT NOT NULL",
            "country_code": "TEXT NOT NULL",
            "first_name": "TEXT NOT NULL",
            "full_name": "TEXT NOT NULL",
            "headshot_url": "TEXT",
            "last_name": "TEXT NOT NULL",
            "name_acronym": "TEXT NOT NULL",
            "team_colour": "TEXT NOT NULL",
            "team_name": "TEXT NOT NULL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_laps": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "lap_number": "INTEGER NOT NULL",
            "date_start": "TIMESTAMP NOT NULL",
            "duration_sector_1": "REAL",
            "duration_sector_2": "REAL",
            "duration_sector_3": "REAL",
            "i1_speed": "INTEGER",
            "i2_speed": "INTEGER",
            "is_pit_out_lap": "BOOLEAN",
            "lap_duration": "REAL",
            "st_speed": "INTEGER",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_car_data": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "date": "TIMESTAMP NOT NULL",
            "brake": "INTEGER",
            "drs": "INTEGER",
            "n_gear": "INTEGER",
            "rpm": "INTEGER",
            "speed": "INTEGER",
            "throttle": "INTEGER",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_position": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "date": "TIMESTAMP NOT NULL",
            "position": "INTEGER NOT NULL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_pit": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "date": "TIMESTAMP NOT NULL",
            "lap_number": "INTEGER NOT NULL",
            "pit_duration": "REAL NOT NULL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        },
        
        "raw_weather": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "session_key": "INTEGER NOT NULL",
            "meeting_key": "INTEGER NOT NULL",
            "date": "TIMESTAMP NOT NULL",
            "air_temperature": "REAL",
            "humidity": "INTEGER",
            "pressure": "REAL",
            "rainfall": "INTEGER",
            "track_temperature": "REAL",
            "wind_direction": "INTEGER",
            "wind_speed": "REAL",
            "year": "INTEGER NOT NULL",
            "_extracted_at": "TIMESTAMP NOT NULL",
            "_endpoint": "TEXT NOT NULL",
            "FOREIGN KEY (session_key)": "REFERENCES raw_sessions(session_key)",
            "FOREIGN KEY (meeting_key)": "REFERENCES raw_meetings(meeting_key)"
        }
    }
    
    # Analytics layer schemas
    ANALYTICS_SCHEMAS = {
        "grand_prix_results": {
            "result_id": "TEXT PRIMARY KEY",
            "date": "TIMESTAMP NOT NULL",
            "year": "INTEGER NOT NULL",
            "grand_prix": "TEXT NOT NULL",
            "circuit_name": "TEXT NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "driver_name": "TEXT NOT NULL",
            "driver_acronym": "TEXT NOT NULL",
            "team_name": "TEXT NOT NULL",
            "starting_grid_position": "INTEGER",
            "final_position": "INTEGER",
            "points": "REAL NOT NULL DEFAULT 0",
            "fastest_lap": "REAL",
            "total_time_penalty": "REAL NOT NULL DEFAULT 0",
            "total_season_points": "REAL NOT NULL DEFAULT 0",
            "drivers_championship_ranking": "INTEGER",
            "points_from_first": "REAL NOT NULL DEFAULT 0",
            "dnf": "BOOLEAN NOT NULL DEFAULT FALSE",
            "dns": "BOOLEAN NOT NULL DEFAULT FALSE",
            "dsq": "BOOLEAN NOT NULL DEFAULT FALSE",
            "created_at": "TIMESTAMP NOT NULL"
        },
        
        "grand_prix_performance": {
            "performance_id": "TEXT PRIMARY KEY",
            "date": "TIMESTAMP NOT NULL",
            "year": "INTEGER NOT NULL",
            "grand_prix": "TEXT NOT NULL",
            "session_name": "TEXT NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "driver_name": "TEXT NOT NULL",
            "best_lap_time": "REAL",
            "average_lap_time": "REAL",
            "total_pit_time": "REAL NOT NULL DEFAULT 0",
            "pit_stops": "INTEGER NOT NULL DEFAULT 0",
            "avg_speed": "REAL",
            "max_speed": "REAL",
            "avg_throttle": "REAL",
            "time_spent_braking_pct": "REAL",
            "tire_compounds_used": "TEXT",  # JSON array as text
            "tire_stint_count": "INTEGER NOT NULL DEFAULT 0",
            "created_at": "TIMESTAMP NOT NULL"
        },
        
        "driver_championship_standings": {
            "standing_id": "TEXT PRIMARY KEY",
            "year": "INTEGER NOT NULL",
            "race_round": "INTEGER NOT NULL",
            "after_race": "TEXT NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "driver_name": "TEXT NOT NULL",
            "team_name": "TEXT NOT NULL",
            "position": "INTEGER NOT NULL",
            "points": "REAL NOT NULL",
            "points_behind_leader": "REAL NOT NULL DEFAULT 0",
            "points_ahead_next": "REAL NOT NULL DEFAULT 0",
            "wins": "INTEGER NOT NULL DEFAULT 0",
            "podiums": "INTEGER NOT NULL DEFAULT 0",
            "points_finishes": "INTEGER NOT NULL DEFAULT 0",
            "created_at": "TIMESTAMP NOT NULL"
        }
    }
    
    # AI preparation layer schemas
    AI_SCHEMAS = {
        "driver_performance_features": {
            "feature_id": "TEXT PRIMARY KEY",
            "year": "INTEGER NOT NULL",
            "race_round": "INTEGER NOT NULL",
            "session_type": "TEXT NOT NULL",
            "driver_number": "INTEGER NOT NULL",
            "pace_vs_teammate": "REAL",
            "pace_vs_field": "REAL",
            "consistency_score": "REAL",
            "track_position_start": "INTEGER",
            "track_position_avg": "REAL",
            "straight_line_speed_rank": "INTEGER",
            "cornering_speed_rank": "INTEGER",
            "air_temperature": "REAL",
            "track_temperature": "REAL",
            "created_at": "TIMESTAMP NOT NULL"
        },
        
        "race_context_features": {
            "context_id": "TEXT PRIMARY KEY",
            "year": "INTEGER NOT NULL",
            "race_round": "INTEGER NOT NULL",
            "grand_prix": "TEXT NOT NULL",
            "circuit_length": "REAL",
            "number_of_turns": "INTEGER",
            "overtaking_difficulty": "REAL",
            "safety_car_deployments": "INTEGER NOT NULL DEFAULT 0",
            "red_flag_periods": "INTEGER NOT NULL DEFAULT 0",
            "total_race_time": "REAL",
            "weather_changes": "BOOLEAN NOT NULL DEFAULT FALSE",
            "rain_probability": "REAL",
            "created_at": "TIMESTAMP NOT NULL"
        }
    }
    
    # Metadata schemas
    METADATA_SCHEMAS = {
        "schema_metadata": {
            "table_name": "TEXT PRIMARY KEY",
            "schema_version": "TEXT NOT NULL",
            "description": "TEXT NOT NULL",
            "data_source": "TEXT NOT NULL",
            "field_descriptions": "TEXT NOT NULL",  # JSON as text
            "field_types": "TEXT NOT NULL",  # JSON as text
            "upstream_dependencies": "TEXT",  # JSON as text
            "downstream_dependencies": "TEXT",  # JSON as text
            "last_updated": "TIMESTAMP NOT NULL",
            "update_frequency": "TEXT",
            "created_at": "TIMESTAMP NOT NULL"
        },
        
        "data_quality_metrics": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "table_name": "TEXT NOT NULL",
            "measurement_date": "TIMESTAMP NOT NULL",
            "total_records": "INTEGER NOT NULL",
            "null_counts": "TEXT NOT NULL",  # JSON as text
            "completeness_score": "REAL NOT NULL",
            "duplicate_records": "INTEGER NOT NULL DEFAULT 0",
            "constraint_violations": "INTEGER NOT NULL DEFAULT 0",
            "latest_data_timestamp": "TIMESTAMP",
            "data_freshness_hours": "REAL",
            "outlier_count": "INTEGER NOT NULL DEFAULT 0",
            "anomaly_score": "REAL",
            "created_at": "TIMESTAMP NOT NULL"
        }
    }
    
    @classmethod
    def get_all_schemas(cls) -> Dict[str, Dict[str, str]]:
        """Get all schema definitions."""
        all_schemas = {}
        all_schemas.update(cls.RAW_SCHEMAS)
        all_schemas.update(cls.ANALYTICS_SCHEMAS)
        all_schemas.update(cls.AI_SCHEMAS)
        all_schemas.update(cls.METADATA_SCHEMAS)
        return all_schemas
    
    @classmethod
    def get_schema_by_layer(cls, layer: str) -> Dict[str, Dict[str, str]]:
        """Get schemas by layer (raw, analytics, ai, metadata)."""
        layer_map = {
            "raw": cls.RAW_SCHEMAS,
            "analytics": cls.ANALYTICS_SCHEMAS,
            "ai": cls.AI_SCHEMAS,
            "metadata": cls.METADATA_SCHEMAS
        }
        
        if layer not in layer_map:
            raise ValueError(f"Unknown layer: {layer}. Available: {list(layer_map.keys())}")
        
        return layer_map[layer]
    
    @classmethod
    def get_create_table_sql(cls, table_name: str, schema: Dict[str, str]) -> str:
        """Generate CREATE TABLE SQL statement."""
        # Separate column definitions from constraints
        columns = []
        constraints = []
        
        for key, value in schema.items():
            if key.startswith("FOREIGN KEY") or key.startswith("UNIQUE") or key.startswith("CHECK"):
                constraints.append(f"{key} {value}")
            else:
                columns.append(f"{key} {value}")
        
        # Combine columns and constraints
        all_definitions = columns + constraints
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    "
        sql += ",\n    ".join(all_definitions)
        sql += "\n)"
        
        return sql
    
    @classmethod
    def get_indexes_sql(cls, table_name: str) -> List[str]:
        """Generate index creation SQL statements for a table."""
        indexes = []
        
        # Common indexes based on table patterns
        if table_name.startswith("raw_"):
            indexes.extend([
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_session_key ON {table_name}(session_key)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_meeting_key ON {table_name}(meeting_key)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year)"
            ])
            
            if "driver_number" in cls.get_all_schemas().get(table_name, {}):
                indexes.append(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_driver_number ON {table_name}(driver_number)")
        
        elif table_name.startswith("grand_prix_"):
            indexes.extend([
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_driver_number ON {table_name}(driver_number)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(date)"
            ])
        
        elif table_name.startswith("driver_"):
            indexes.extend([
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON {table_name}(year)",
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_driver_number ON {table_name}(driver_number)"
            ])
        
        return indexes
    
    @classmethod
    def validate_schema(cls, table_name: str, data_dict: Dict[str, Any]) -> List[str]:
        """Validate data against schema and return list of validation errors."""
        errors = []
        
        all_schemas = cls.get_all_schemas()
        if table_name not in all_schemas:
            return [f"Table {table_name} not found in schema definitions"]
        
        schema = all_schemas[table_name]
        
        # Check for required fields (those marked as NOT NULL without DEFAULT)
        for field_name, field_def in schema.items():
            if field_name.startswith("FOREIGN KEY") or field_name.startswith("UNIQUE"):
                continue
                
            if "NOT NULL" in field_def and "DEFAULT" not in field_def and "AUTOINCREMENT" not in field_def:
                if field_name not in data_dict or data_dict[field_name] is None:
                    errors.append(f"Required field {field_name} is missing or null")
        
        # Type validation could be added here
        
        return errors