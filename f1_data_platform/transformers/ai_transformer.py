"""AI preparation layer transformations."""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
import json

from ..cloud_swap import CloudProvider
from ..models.schemas import SchemaManager
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class AIPreparationTransformer:
    """Transforms analytics data into AI-ready feature sets."""

    def __init__(self, settings: Settings, cloud_provider: CloudProvider):
        """Initialize the AI preparation transformer.
        
        Args:
            settings: Application settings
            cloud_provider: Cloud provider instance
        """
        self.settings = settings
        self.cloud_provider = cloud_provider
        self.storage = cloud_provider.get_storage_provider()
        self.database = cloud_provider.get_database_provider()
        self.schema_manager = SchemaManager()

    def setup_ai_tables(self) -> Dict[str, bool]:
        """Create AI preparation layer tables in the database.
        
        Returns:
            Dictionary mapping table names to creation success status
        """
        logger.info("Setting up AI preparation layer tables")
        
        results = {}
        ai_schemas = self.schema_manager.get_schema_by_layer("ai")
        
        for table_name, schema in ai_schemas.items():
            try:
                # Create table
                create_sql = self.schema_manager.get_create_table_sql(table_name, schema)
                self.database.execute_query(create_sql)
                
                # Create indexes
                index_sqls = self.schema_manager.get_indexes_sql(table_name)
                for index_sql in index_sqls:
                    self.database.execute_query(index_sql)
                
                results[table_name] = True
                logger.info(f"Created AI table: {table_name}")
                
            except Exception as e:
                logger.error(f"Failed to create AI table {table_name}: {e}")
                results[table_name] = False
        
        return results

    def create_driver_performance_features(self, year: int) -> pd.DataFrame:
        """Create feature-engineered dataset for driver performance analysis.
        
        Args:
            year: Year to process
            
        Returns:
            DataFrame with driver performance features
        """
        logger.info(f"Creating driver performance features for year {year}")
        
        try:
            # Get all performance data for the year
            performance_query = """
                SELECT p.*, r.final_position, r.starting_grid_position, r.points
                FROM grand_prix_performance p
                LEFT JOIN grand_prix_results r ON (
                    p.year = r.year AND 
                    p.grand_prix = r.grand_prix AND 
                    p.driver_number = r.driver_number AND
                    p.session_name = 'Race'
                )
                WHERE p.year = ?
                ORDER BY p.date, p.driver_number
            """
            performance_data = self.database.fetch_dataframe(performance_query, {"year": year})
            
            if performance_data.empty:
                logger.warning(f"No performance data found for year {year}")
                return pd.DataFrame()
            
            # Get weather data for context
            weather_query = """
                SELECT DISTINCT w.session_key, w.air_temperature, w.track_temperature,
                       AVG(w.air_temperature) as avg_air_temp,
                       AVG(w.track_temperature) as avg_track_temp
                FROM raw_weather w
                JOIN raw_sessions s ON w.session_key = s.session_key
                WHERE s.year = ?
                GROUP BY w.session_key
            """
            weather_data = self.database.fetch_dataframe(weather_query, {"year": year})
            
            features = []
            
            # Group by Grand Prix and session
            for (grand_prix, session_name), session_group in performance_data.groupby(["grand_prix", "session_name"]):
                race_round = self._get_race_round(year, grand_prix)
                
                # Calculate relative performance metrics
                session_features = self._calculate_session_features(session_group, race_round)
                
                # Add weather context
                session_key = self._get_session_key(year, grand_prix, session_name)
                weather_context = self._get_weather_context(weather_data, session_key)
                
                # Combine features
                for feature in session_features:
                    feature.update(weather_context)
                    features.extend(session_features)
            
            if not features:
                logger.warning(f"No features generated for year {year}")
                return pd.DataFrame()
            
            features_df = pd.DataFrame(features)
            logger.info(f"Generated {len(features_df)} driver performance features for year {year}")
            return features_df
            
        except Exception as e:
            logger.error(f"Error creating driver performance features for year {year}: {e}")
            return pd.DataFrame()

    def create_race_context_features(self, year: int) -> pd.DataFrame:
        """Create race context features for ML models.
        
        Args:
            year: Year to process
            
        Returns:
            DataFrame with race context features
        """
        logger.info(f"Creating race context features for year {year}")
        
        try:
            # Get race sessions for the year
            races_query = """
                SELECT s.*, m.meeting_name, m.circuit_short_name
                FROM raw_sessions s
                JOIN raw_meetings m ON s.meeting_key = m.meeting_key
                WHERE s.year = ? AND s.session_type = 'Race'
                ORDER BY s.date_start
            """
            races = self.database.fetch_dataframe(races_query, {"year": year})
            
            if races.empty:
                logger.warning(f"No race sessions found for year {year}")
                return pd.DataFrame()
            
            context_features = []
            
            for race_idx, race in races.iterrows():
                session_key = race["session_key"]
                
                # Calculate race characteristics
                race_features = {
                    "context_id": f"{year}_{race_idx + 1}",
                    "year": year,
                    "race_round": race_idx + 1,
                    "grand_prix": race["meeting_name"],
                }
                
                # Track characteristics (these would ideally come from external data)
                track_features = self._calculate_track_features(race["circuit_short_name"])
                race_features.update(track_features)
                
                # Race event characteristics
                race_events = self._calculate_race_events(session_key)
                race_features.update(race_events)
                
                # Weather characteristics
                weather_features = self._calculate_weather_features(session_key)
                race_features.update(weather_features)
                
                race_features["created_at"] = datetime.utcnow()
                context_features.append(race_features)
            
            if not context_features:
                logger.warning(f"No context features generated for year {year}")
                return pd.DataFrame()
            
            context_df = pd.DataFrame(context_features)
            logger.info(f"Generated {len(context_df)} race context features for year {year}")
            return context_df
            
        except Exception as e:
            logger.error(f"Error creating race context features for year {year}: {e}")
            return pd.DataFrame()

    def create_comparative_analysis_dataset(self, year: int, 
                                          analysis_type: str = "car_behind_impact") -> pd.DataFrame:
        """Create dataset for comparative analysis (like car behind impact analysis).
        
        Args:
            year: Year to analyze
            analysis_type: Type of analysis to prepare data for
            
        Returns:
            DataFrame prepared for the specific analysis
        """
        logger.info(f"Creating {analysis_type} dataset for year {year}")
        
        if analysis_type == "car_behind_impact":
            return self._create_car_behind_impact_dataset(year)
        else:
            logger.warning(f"Unknown analysis type: {analysis_type}")
            return pd.DataFrame()

    def _create_car_behind_impact_dataset(self, year: int) -> pd.DataFrame:
        """Create dataset for analyzing impact of car behind on performance.
        
        This addresses the specific use case mentioned in requirements:
        "Compare driver's performance to the car's in front of them for the last 5 races"
        """
        logger.info(f"Creating car behind impact dataset for year {year}")
        
        try:
            # Get lap data with position information
            lap_data_query = """
                SELECT l.*, p.position, s.session_name, s.date_start, m.meeting_name
                FROM raw_laps l
                JOIN raw_position p ON (
                    l.session_key = p.session_key AND 
                    l.driver_number = p.driver_number AND
                    ABS(strftime('%s', l.date_start) - strftime('%s', p.date)) < 30
                )
                JOIN raw_sessions s ON l.session_key = s.session_key
                JOIN raw_meetings m ON s.meeting_key = m.meeting_key
                WHERE l.year = ? AND s.session_type = 'Race'
                AND l.lap_duration IS NOT NULL
                ORDER BY l.session_key, l.lap_number, p.position
            """
            lap_data = self.database.fetch_dataframe(lap_data_query, {"year": year})
            
            if lap_data.empty:
                logger.warning(f"No lap data found for car behind analysis for year {year}")
                return pd.DataFrame()
            
            # Get tire age data
            tire_data_query = """
                SELECT session_key, driver_number, lap_start, lap_end, 
                       compound, tyre_age_at_start
                FROM raw_stints
                WHERE session_key IN (
                    SELECT DISTINCT session_key FROM raw_sessions 
                    WHERE year = ? AND session_type = 'Race'
                )
            """
            tire_data = self.database.fetch_dataframe(tire_data_query, {"year": year})
            
            analysis_records = []
            
            # Group by session and analyze lap by lap
            for session_key, session_laps in lap_data.groupby("session_key"):
                session_analysis = self._analyze_session_car_impact(session_laps, tire_data)
                analysis_records.extend(session_analysis)
            
            if not analysis_records:
                logger.warning(f"No car behind impact records generated for year {year}")
                return pd.DataFrame()
            
            analysis_df = pd.DataFrame(analysis_records)
            logger.info(f"Generated {len(analysis_df)} car behind impact records for year {year}")
            return analysis_df
            
        except Exception as e:
            logger.error(f"Error creating car behind impact dataset for year {year}: {e}")
            return pd.DataFrame()

    def save_ai_features(self, year: int) -> Dict[str, bool]:
        """Generate and save all AI features for a year.
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary indicating success status for each AI feature set
        """
        logger.info(f"Saving AI features for year {year}")
        
        results = {}
        
        # Create and save driver performance features
        try:
            driver_features = self.create_driver_performance_features(year)
            if not driver_features.empty:
                success = self.database.insert_dataframe(
                    driver_features, "driver_performance_features", if_exists="append"
                )
                results["driver_performance_features"] = success
                if success:
                    logger.info(f"Saved {len(driver_features)} driver performance features for year {year}")
            else:
                results["driver_performance_features"] = False
        except Exception as e:
            logger.error(f"Error saving driver performance features for year {year}: {e}")
            results["driver_performance_features"] = False
        
        # Create and save race context features
        try:
            context_features = self.create_race_context_features(year)
            if not context_features.empty:
                success = self.database.insert_dataframe(
                    context_features, "race_context_features", if_exists="append"
                )
                results["race_context_features"] = success
                if success:
                    logger.info(f"Saved {len(context_features)} race context features for year {year}")
            else:
                results["race_context_features"] = False
        except Exception as e:
            logger.error(f"Error saving race context features for year {year}: {e}")
            results["race_context_features"] = False
        
        return results

    # Helper methods
    
    def _calculate_session_features(self, session_group: pd.DataFrame, race_round: int) -> List[Dict[str, Any]]:
        """Calculate performance features for a session."""
        features = []
        
        # Calculate team-based comparisons
        for team_name, team_group in session_group.groupby("team_name"):
            if len(team_group) >= 2:  # Need at least 2 drivers for comparison
                drivers = team_group.sort_values("best_lap_time")
                
                for i, (_, driver) in enumerate(drivers.iterrows()):
                    teammate_times = drivers[drivers.index != driver.name]["best_lap_time"].dropna()
                    if not teammate_times.empty:
                        pace_vs_teammate = driver["best_lap_time"] - teammate_times.mean()
                    else:
                        pace_vs_teammate = None
                    
                    # Calculate field comparison
                    field_avg = session_group["best_lap_time"].mean()
                    pace_vs_field = driver["best_lap_time"] - field_avg if pd.notna(driver["best_lap_time"]) else None
                    
                    # Calculate consistency (would need lap-by-lap data)
                    consistency_score = None  # Placeholder
                    
                    feature = {
                        "feature_id": f"{driver['year']}_{race_round}_{driver['session_name']}_{driver['driver_number']}",
                        "year": driver["year"],
                        "race_round": race_round,
                        "session_type": driver["session_name"],
                        "driver_number": driver["driver_number"],
                        "pace_vs_teammate": pace_vs_teammate,
                        "pace_vs_field": pace_vs_field,
                        "consistency_score": consistency_score,
                        "track_position_start": driver.get("starting_grid_position"),
                        "track_position_avg": driver.get("final_position"),  # Simplified
                        "created_at": datetime.utcnow()
                    }
                    
                    features.append(feature)
        
        return features
    
    def _get_race_round(self, year: int, grand_prix: str) -> int:
        """Get race round number for a Grand Prix."""
        # This would ideally come from a proper race calendar
        # For now, use a simple hash-based approach
        return hash(grand_prix) % 23 + 1
    
    def _get_session_key(self, year: int, grand_prix: str, session_name: str) -> Optional[int]:
        """Get session key for a specific session."""
        try:
            query = """
                SELECT s.session_key
                FROM raw_sessions s
                JOIN raw_meetings m ON s.meeting_key = m.meeting_key
                WHERE s.year = ? AND m.meeting_name = ? AND s.session_name = ?
            """
            result = self.database.fetch_dataframe(query, {
                "year": year, 
                "meeting_name": grand_prix, 
                "session_name": session_name
            })
            return result.iloc[0]["session_key"] if not result.empty else None
        except Exception:
            return None
    
    def _get_weather_context(self, weather_data: pd.DataFrame, session_key: Optional[int]) -> Dict[str, Any]:
        """Get weather context for a session."""
        if session_key is None or weather_data.empty:
            return {"air_temperature": None, "track_temperature": None}
        
        session_weather = weather_data[weather_data["session_key"] == session_key]
        if session_weather.empty:
            return {"air_temperature": None, "track_temperature": None}
        
        return {
            "air_temperature": session_weather.iloc[0]["avg_air_temp"],
            "track_temperature": session_weather.iloc[0]["avg_track_temp"]
        }
    
    def _calculate_track_features(self, circuit_name: str) -> Dict[str, Any]:
        """Calculate track characteristics (placeholder - would use external data)."""
        # This would ideally come from a track database
        track_characteristics = {
            "Monaco": {"circuit_length": 3.337, "number_of_turns": 19, "overtaking_difficulty": 0.9},
            "Monza": {"circuit_length": 5.793, "number_of_turns": 11, "overtaking_difficulty": 0.3},
            "Silverstone": {"circuit_length": 5.891, "number_of_turns": 18, "overtaking_difficulty": 0.5},
            # Add more tracks as needed
        }
        
        return track_characteristics.get(circuit_name, {
            "circuit_length": None,
            "number_of_turns": None, 
            "overtaking_difficulty": None
        })
    
    def _calculate_race_events(self, session_key: int) -> Dict[str, Any]:
        """Calculate race event characteristics."""
        try:
            # Count safety cars and red flags from race control data
            race_control_query = """
                SELECT category, flag, COUNT(*) as count
                FROM raw_race_control
                WHERE session_key = ?
                AND (category LIKE '%Safety%' OR flag LIKE '%RED%')
                GROUP BY category, flag
            """
            race_control = self.database.fetch_dataframe(race_control_query, {"session_key": session_key})
            
            safety_cars = len(race_control[race_control["category"].str.contains("Safety", na=False)])
            red_flags = len(race_control[race_control["flag"].str.contains("RED", na=False)])
            
            # Get total race time (would need lap data aggregation)
            total_race_time = None  # Placeholder
            
            return {
                "safety_car_deployments": safety_cars,
                "red_flag_periods": red_flags,
                "total_race_time": total_race_time
            }
        except Exception:
            return {
                "safety_car_deployments": 0,
                "red_flag_periods": 0,
                "total_race_time": None
            }
    
    def _calculate_weather_features(self, session_key: int) -> Dict[str, Any]:
        """Calculate weather characteristics for a session."""
        try:
            weather_query = """
                SELECT air_temperature, track_temperature, rainfall
                FROM raw_weather
                WHERE session_key = ?
                ORDER BY date
            """
            weather = self.database.fetch_dataframe(weather_query, {"session_key": session_key})
            
            if weather.empty:
                return {"weather_changes": False, "rain_probability": None}
            
            # Detect weather changes
            temp_variance = weather["air_temperature"].var() if len(weather) > 1 else 0
            weather_changes = temp_variance > 5  # Threshold for significant change
            
            # Rain probability
            rain_probability = (weather["rainfall"] > 0).mean() if "rainfall" in weather.columns else 0
            
            return {
                "weather_changes": weather_changes,
                "rain_probability": rain_probability
            }
        except Exception:
            return {"weather_changes": False, "rain_probability": None}
    
    def _analyze_session_car_impact(self, session_laps: pd.DataFrame, 
                                   tire_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze the impact of cars behind on lap times."""
        analysis_records = []
        
        # Group by lap and analyze position-based performance
        for lap_number, lap_group in session_laps.groupby("lap_number"):
            lap_positions = lap_group.sort_values("position")
            
            for i, (_, current_driver) in enumerate(lap_positions.iterrows()):
                # Find car behind (next position)
                car_behind = None
                if i < len(lap_positions) - 1:
                    car_behind = lap_positions.iloc[i + 1]
                
                # Find car ahead (previous position) 
                car_ahead = None
                if i > 0:
                    car_ahead = lap_positions.iloc[i - 1]
                
                # Get tire age for current driver
                tire_age = self._get_tire_age(
                    tire_data, current_driver["session_key"], 
                    current_driver["driver_number"], lap_number
                )
                
                # Calculate gap to car behind
                gap_to_behind = None
                if car_behind is not None:
                    gap_to_behind = car_behind["lap_duration"] - current_driver["lap_duration"]
                
                # Calculate impact metrics
                analysis_record = {
                    "session_key": current_driver["session_key"],
                    "lap_number": lap_number,
                    "driver_number": current_driver["driver_number"],
                    "position": current_driver["position"],
                    "lap_time": current_driver["lap_duration"],
                    "tire_age": tire_age,
                    "has_car_behind": car_behind is not None,
                    "gap_to_behind": gap_to_behind,
                    "car_behind_driver": car_behind["driver_number"] if car_behind is not None else None,
                    "has_car_ahead": car_ahead is not None,
                    "car_ahead_driver": car_ahead["driver_number"] if car_ahead is not None else None,
                    "meeting_name": current_driver["meeting_name"],
                    "created_at": datetime.utcnow()
                }
                
                analysis_records.append(analysis_record)
        
        return analysis_records
    
    def _get_tire_age(self, tire_data: pd.DataFrame, session_key: int, 
                     driver_number: int, lap_number: int) -> Optional[int]:
        """Get tire age for a specific lap."""
        if tire_data.empty:
            return None
        
        driver_stints = tire_data[
            (tire_data["session_key"] == session_key) & 
            (tire_data["driver_number"] == driver_number)
        ]
        
        for _, stint in driver_stints.iterrows():
            if stint["lap_start"] <= lap_number <= stint["lap_end"]:
                return stint["tyre_age_at_start"] + (lap_number - stint["lap_start"])
        
        return None