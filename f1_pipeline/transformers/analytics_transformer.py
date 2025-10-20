"""Data transformation pipeline for F1 analytics."""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import json

from ..cloud_swap import CloudProvider
from ..models.schemas import SchemaManager
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms raw F1 data into analytics and AI-ready formats."""

    def __init__(self, settings: Settings, cloud_provider: CloudProvider):
        """Initialize the data transformer.
        
        Args:
            settings: Application settings
            cloud_provider: Cloud provider instance
        """
        self.settings = settings
        self.cloud_provider = cloud_provider
        self.storage = cloud_provider.get_storage_provider()
        self.database = cloud_provider.get_database_provider()
        self.schema_manager = SchemaManager()

    def setup_analytics_tables(self) -> Dict[str, bool]:
        """Create analytics layer tables in the database.
        
        Returns:
            Dictionary mapping table names to creation success status
        """
        logger.info("Setting up analytics layer tables")
        
        results = {}
        analytics_schemas = self.schema_manager.get_schema_by_layer("analytics")
        
        for table_name, schema in analytics_schemas.items():
            try:
                # Create table
                create_sql = self.schema_manager.get_create_table_sql(table_name, schema)
                self.database.execute_query(create_sql)
                
                # Create indexes
                index_sqls = self.schema_manager.get_indexes_sql(table_name)
                for index_sql in index_sqls:
                    self.database.execute_query(index_sql)
                
                results[table_name] = True
                logger.info(f"Created analytics table: {table_name}")
                
            except Exception as e:
                logger.error(f"Failed to create analytics table {table_name}: {e}")
                results[table_name] = False
        
        return results

    def transform_grand_prix_results(self, year: int) -> pd.DataFrame:
        """Transform raw data into Grand Prix results.
        
        Args:
            year: Year to process
            
        Returns:
            DataFrame with transformed Grand Prix results
        """
        logger.info(f"Transforming Grand Prix results for year {year}")
        
        try:
            # Get race sessions for the year
            race_sessions_query = """
                SELECT s.*, m.meeting_name, m.circuit_short_name, m.date_start as race_date
                FROM raw_sessions s
                JOIN raw_meetings m ON s.meeting_key = m.meeting_key
                WHERE s.year = ? AND s.session_type = 'Race'
                ORDER BY s.date_start
            """
            race_sessions = self.database.fetch_dataframe(race_sessions_query, {"year": year})
            
            if race_sessions.empty:
                logger.warning(f"No race sessions found for year {year}")
                return pd.DataFrame()
            
            results = []
            
            for _, session in race_sessions.iterrows():
                session_key = session["session_key"]
                meeting_key = session["meeting_key"]
                
                # Get session results
                session_results = self._get_session_results(session_key)
                if session_results.empty:
                    continue
                
                # Get drivers info
                drivers = self._get_session_drivers(session_key)
                if drivers.empty:
                    continue
                
                # Get starting grid positions
                starting_grid = self._get_starting_grid(session_key)
                
                # Get fastest laps
                fastest_laps = self._get_fastest_laps(session_key)
                
                # Get penalties (from race control)
                penalties = self._get_penalties(session_key)
                
                # Merge all data
                for _, result in session_results.iterrows():
                    driver_number = result["driver_number"]
                    
                    # Get driver info
                    driver_info = drivers[drivers["driver_number"] == driver_number]
                    if driver_info.empty:
                        continue
                    driver_info = driver_info.iloc[0]
                    
                    # Get starting position
                    start_pos = starting_grid[starting_grid["driver_number"] == driver_number]
                    starting_position = start_pos.iloc[0]["position"] if not start_pos.empty else None
                    
                    # Get fastest lap
                    fast_lap = fastest_laps[fastest_laps["driver_number"] == driver_number]
                    fastest_lap_time = fast_lap.iloc[0]["lap_duration"] if not fast_lap.empty else None
                    
                    # Get penalties
                    driver_penalties = penalties[penalties["driver_number"] == driver_number]
                    total_penalty = driver_penalties["penalty_seconds"].sum() if not driver_penalties.empty else 0
                    
                    # Calculate points (F1 2019-2023 point system)
                    points = self._calculate_points(result["position"], fastest_lap_time is not None and result["position"] <= 10)
                    
                    # Create result record
                    result_record = {
                        "result_id": f"{year}_{session['race_round']}_{driver_number}",
                        "date": session["race_date"],
                        "year": year,
                        "grand_prix": session["meeting_name"],
                        "circuit_name": session["circuit_short_name"],
                        "driver_number": driver_number,
                        "driver_name": driver_info["full_name"],
                        "driver_acronym": driver_info["name_acronym"],
                        "team_name": driver_info["team_name"],
                        "starting_grid_position": starting_position,
                        "final_position": result["position"],
                        "points": points,
                        "fastest_lap": fastest_lap_time,
                        "total_time_penalty": total_penalty,
                        "dnf": result.get("dnf", False),
                        "dns": result.get("dns", False),
                        "dsq": result.get("dsq", False),
                        "created_at": datetime.utcnow()
                    }
                    
                    results.append(result_record)
            
            if not results:
                logger.warning(f"No results generated for year {year}")
                return pd.DataFrame()
            
            results_df = pd.DataFrame(results)
            
            # Calculate championship context
            results_df = self._add_championship_context(results_df)
            
            logger.info(f"Generated {len(results_df)} Grand Prix results for year {year}")
            return results_df
            
        except Exception as e:
            logger.error(f"Error transforming Grand Prix results for year {year}: {e}")
            return pd.DataFrame()

    def transform_grand_prix_performance(self, year: int) -> pd.DataFrame:
        """Transform raw data into Grand Prix performance metrics.
        
        Args:
            year: Year to process
            
        Returns:
            DataFrame with performance metrics
        """
        logger.info(f"Transforming Grand Prix performance for year {year}")
        
        try:
            # Get all sessions for the year
            sessions_query = """
                SELECT s.*, m.meeting_name, m.circuit_short_name
                FROM raw_sessions s
                JOIN raw_meetings m ON s.meeting_key = m.meeting_key
                WHERE s.year = ?
                ORDER BY s.date_start
            """
            sessions = self.database.fetch_dataframe(sessions_query, {"year": year})
            
            if sessions.empty:
                logger.warning(f"No sessions found for year {year}")
                return pd.DataFrame()
            
            performance_records = []
            
            for _, session in sessions.iterrows():
                session_key = session["session_key"]
                
                # Get drivers for this session
                drivers = self._get_session_drivers(session_key)
                if drivers.empty:
                    continue
                
                for _, driver in drivers.iterrows():
                    driver_number = driver["driver_number"]
                    
                    # Get lap performance
                    lap_performance = self._calculate_lap_performance(session_key, driver_number)
                    
                    # Get pit performance
                    pit_performance = self._calculate_pit_performance(session_key, driver_number)
                    
                    # Get telemetry aggregates
                    telemetry_agg = self._calculate_telemetry_aggregates(session_key, driver_number)
                    
                    # Get tire information
                    tire_info = self._calculate_tire_performance(session_key, driver_number)
                    
                    performance_record = {
                        "performance_id": f"{year}_{session_key}_{driver_number}",
                        "date": session["date_start"],
                        "year": year,
                        "grand_prix": session["meeting_name"],
                        "session_name": session["session_name"],
                        "driver_number": driver_number,
                        "driver_name": driver["full_name"],
                        
                        # Lap performance
                        "best_lap_time": lap_performance.get("best_lap_time"),
                        "average_lap_time": lap_performance.get("average_lap_time"),
                        
                        # Pit performance
                        "total_pit_time": pit_performance.get("total_pit_time", 0),
                        "pit_stops": pit_performance.get("pit_stops", 0),
                        
                        # Telemetry aggregates
                        "avg_speed": telemetry_agg.get("avg_speed"),
                        "max_speed": telemetry_agg.get("max_speed"),
                        "avg_throttle": telemetry_agg.get("avg_throttle"),
                        "time_spent_braking_pct": telemetry_agg.get("time_spent_braking_pct"),
                        
                        # Tire information
                        "tire_compounds_used": json.dumps(tire_info.get("compounds_used", [])),
                        "tire_stint_count": tire_info.get("stint_count", 0),
                        
                        "created_at": datetime.utcnow()
                    }
                    
                    performance_records.append(performance_record)
            
            if not performance_records:
                logger.warning(f"No performance records generated for year {year}")
                return pd.DataFrame()
            
            performance_df = pd.DataFrame(performance_records)
            logger.info(f"Generated {len(performance_df)} performance records for year {year}")
            return performance_df
            
        except Exception as e:
            logger.error(f"Error transforming Grand Prix performance for year {year}: {e}")
            return pd.DataFrame()

    def transform_championship_standings(self, year: int) -> pd.DataFrame:
        """Transform Grand Prix results into championship standings.
        
        Args:
            year: Year to process
            
        Returns:
            DataFrame with championship standings after each race
        """
        logger.info(f"Transforming championship standings for year {year}")
        
        try:
            # Get all Grand Prix results for the year, ordered by date
            results_query = """
                SELECT * FROM grand_prix_results
                WHERE year = ?
                ORDER BY date, final_position
            """
            results = self.database.fetch_dataframe(results_query, {"year": year})
            
            if results.empty:
                logger.warning(f"No Grand Prix results found for year {year}")
                return pd.DataFrame()
            
            # Group by race and calculate cumulative standings
            standings_records = []
            races = results.groupby(["date", "grand_prix"]).first().reset_index()
            
            cumulative_points = {}  # driver_number -> points
            
            for race_idx, race in races.iterrows():
                race_results = results[
                    (results["date"] == race["date"]) & 
                    (results["grand_prix"] == race["grand_prix"])
                ]
                
                # Update cumulative points
                for _, result in race_results.iterrows():
                    driver_number = result["driver_number"]
                    if driver_number not in cumulative_points:
                        cumulative_points[driver_number] = 0
                    cumulative_points[driver_number] += result["points"]
                
                # Create standings for this race
                race_standings = []
                for driver_number, total_points in cumulative_points.items():
                    # Get driver info from results
                    driver_info = results[results["driver_number"] == driver_number].iloc[0]
                    
                    # Calculate wins and podiums up to this point
                    driver_results = results[
                        (results["driver_number"] == driver_number) & 
                        (results["date"] <= race["date"])
                    ]
                    
                    wins = len(driver_results[driver_results["final_position"] == 1])
                    podiums = len(driver_results[driver_results["final_position"] <= 3])
                    points_finishes = len(driver_results[driver_results["points"] > 0])
                    
                    race_standings.append({
                        "driver_number": driver_number,
                        "driver_name": driver_info["driver_name"],
                        "team_name": driver_info["team_name"],
                        "points": total_points,
                        "wins": wins,
                        "podiums": podiums,
                        "points_finishes": points_finishes
                    })
                
                # Sort by points (descending) and assign positions
                race_standings.sort(key=lambda x: x["points"], reverse=True)
                
                leader_points = race_standings[0]["points"] if race_standings else 0
                
                for position, standing in enumerate(race_standings, 1):
                    points_behind_leader = leader_points - standing["points"]
                    
                    # Points ahead of next driver
                    points_ahead_next = 0
                    if position < len(race_standings):
                        points_ahead_next = standing["points"] - race_standings[position]["points"]
                    
                    standing_record = {
                        "standing_id": f"{year}_{race_idx + 1}_{standing['driver_number']}",
                        "year": year,
                        "race_round": race_idx + 1,
                        "after_race": race["grand_prix"],
                        "driver_number": standing["driver_number"],
                        "driver_name": standing["driver_name"],
                        "team_name": standing["team_name"],
                        "position": position,
                        "points": standing["points"],
                        "points_behind_leader": points_behind_leader,
                        "points_ahead_next": points_ahead_next,
                        "wins": standing["wins"],
                        "podiums": standing["podiums"],
                        "points_finishes": standing["points_finishes"],
                        "created_at": datetime.utcnow()
                    }
                    
                    standings_records.append(standing_record)
            
            if not standings_records:
                logger.warning(f"No championship standings generated for year {year}")
                return pd.DataFrame()
            
            standings_df = pd.DataFrame(standings_records)
            logger.info(f"Generated {len(standings_df)} championship standings records for year {year}")
            return standings_df
            
        except Exception as e:
            logger.error(f"Error transforming championship standings for year {year}: {e}")
            return pd.DataFrame()

    def save_analytics_data(self, year: int) -> Dict[str, bool]:
        """Transform and save all analytics data for a year.
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary indicating success status for each analytics table
        """
        logger.info(f"Saving analytics data for year {year}")
        
        results = {}
        
        # Transform and save Grand Prix results
        try:
            gp_results = self.transform_grand_prix_results(year)
            if not gp_results.empty:
                success = self.database.insert_dataframe(gp_results, "grand_prix_results", if_exists="append")
                results["grand_prix_results"] = success
                if success:
                    logger.info(f"Saved {len(gp_results)} Grand Prix results for year {year}")
            else:
                results["grand_prix_results"] = False
        except Exception as e:
            logger.error(f"Error saving Grand Prix results for year {year}: {e}")
            results["grand_prix_results"] = False
        
        # Transform and save Grand Prix performance
        try:
            gp_performance = self.transform_grand_prix_performance(year)
            if not gp_performance.empty:
                success = self.database.insert_dataframe(gp_performance, "grand_prix_performance", if_exists="append")
                results["grand_prix_performance"] = success
                if success:
                    logger.info(f"Saved {len(gp_performance)} performance records for year {year}")
            else:
                results["grand_prix_performance"] = False
        except Exception as e:
            logger.error(f"Error saving Grand Prix performance for year {year}: {e}")
            results["grand_prix_performance"] = False
        
        # Transform and save championship standings (only if we have Grand Prix results)
        if results.get("grand_prix_results", False):
            try:
                standings = self.transform_championship_standings(year)
                if not standings.empty:
                    success = self.database.insert_dataframe(standings, "driver_championship_standings", if_exists="append")
                    results["driver_championship_standings"] = success
                    if success:
                        logger.info(f"Saved {len(standings)} championship standings for year {year}")
                else:
                    results["driver_championship_standings"] = False
            except Exception as e:
                logger.error(f"Error saving championship standings for year {year}: {e}")
                results["driver_championship_standings"] = False
        else:
            results["driver_championship_standings"] = False
        
        return results

    # Helper methods
    
    def _get_session_results(self, session_key: int) -> pd.DataFrame:
        """Get session results for a specific session."""
        query = """
            SELECT * FROM raw_session_result 
            WHERE session_key = ?
            ORDER BY position
        """
        return self.database.fetch_dataframe(query, {"session_key": session_key})
    
    def _get_session_drivers(self, session_key: int) -> pd.DataFrame:
        """Get drivers for a specific session."""
        query = """
            SELECT DISTINCT driver_number, full_name, name_acronym, team_name
            FROM raw_drivers 
            WHERE session_key = ?
        """
        return self.database.fetch_dataframe(query, {"session_key": session_key})
    
    def _get_starting_grid(self, session_key: int) -> pd.DataFrame:
        """Get starting grid positions for a session."""
        query = """
            SELECT * FROM raw_starting_grid 
            WHERE session_key = ?
            ORDER BY position
        """
        return self.database.fetch_dataframe(query, {"session_key": session_key})
    
    def _get_fastest_laps(self, session_key: int) -> pd.DataFrame:
        """Get fastest laps for each driver in a session."""
        query = """
            SELECT driver_number, MIN(lap_duration) as lap_duration
            FROM raw_laps 
            WHERE session_key = ? AND lap_duration IS NOT NULL
            GROUP BY driver_number
        """
        return self.database.fetch_dataframe(query, {"session_key": session_key})
    
    def _get_penalties(self, session_key: int) -> pd.DataFrame:
        """Get penalties for drivers in a session."""
        # This would need to be parsed from race control messages
        # For now, return empty DataFrame
        return pd.DataFrame(columns=["driver_number", "penalty_seconds"])
    
    def _calculate_points(self, position: int, has_fastest_lap: bool = False) -> float:
        """Calculate F1 points based on position."""
        points_system = {
            1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
            6: 8, 7: 6, 8: 4, 9: 2, 10: 1
        }
        
        points = points_system.get(position, 0)
        
        # Fastest lap bonus (if finished in top 10)
        if has_fastest_lap and position <= 10:
            points += 1
        
        return points
    
    def _add_championship_context(self, results_df: pd.DataFrame) -> pd.DataFrame:
        """Add championship context to results."""
        # Calculate running totals and rankings
        results_df = results_df.sort_values(["driver_number", "date"])
        
        # Calculate cumulative points
        results_df["total_season_points"] = results_df.groupby("driver_number")["points"].cumsum()
        
        # Calculate championship rankings and points from first
        for _, race_date in results_df[["date"]].drop_duplicates().iterrows():
            race_results = results_df[results_df["date"] == race_date["date"]].copy()
            race_results = race_results.sort_values("total_season_points", ascending=False)
            
            # Assign rankings
            rankings = {row["driver_number"]: idx + 1 for idx, (_, row) in enumerate(race_results.iterrows())}
            leader_points = race_results.iloc[0]["total_season_points"] if not race_results.empty else 0
            
            # Update main dataframe
            for driver_num, ranking in rankings.items():
                mask = (results_df["driver_number"] == driver_num) & (results_df["date"] == race_date["date"])
                results_df.loc[mask, "drivers_championship_ranking"] = ranking
                
                driver_points = race_results[race_results["driver_number"] == driver_num]["total_season_points"].iloc[0]
                results_df.loc[mask, "points_from_first"] = leader_points - driver_points
        
        return results_df
    
    def _calculate_lap_performance(self, session_key: int, driver_number: int) -> Dict[str, Any]:
        """Calculate lap performance metrics for a driver in a session."""
        query = """
            SELECT lap_duration 
            FROM raw_laps 
            WHERE session_key = ? AND driver_number = ? 
            AND lap_duration IS NOT NULL
            AND is_pit_out_lap = 0
        """
        laps = self.database.fetch_dataframe(query, {"session_key": session_key, "driver_number": driver_number})
        
        if laps.empty:
            return {}
        
        return {
            "best_lap_time": laps["lap_duration"].min(),
            "average_lap_time": laps["lap_duration"].mean()
        }
    
    def _calculate_pit_performance(self, session_key: int, driver_number: int) -> Dict[str, Any]:
        """Calculate pit performance metrics."""
        query = """
            SELECT pit_duration 
            FROM raw_pit 
            WHERE session_key = ? AND driver_number = ?
        """
        pits = self.database.fetch_dataframe(query, {"session_key": session_key, "driver_number": driver_number})
        
        if pits.empty:
            return {"total_pit_time": 0, "pit_stops": 0}
        
        return {
            "total_pit_time": pits["pit_duration"].sum(),
            "pit_stops": len(pits)
        }
    
    def _calculate_telemetry_aggregates(self, session_key: int, driver_number: int) -> Dict[str, Any]:
        """Calculate telemetry aggregates for a driver."""
        query = """
            SELECT speed, throttle, brake 
            FROM raw_car_data 
            WHERE session_key = ? AND driver_number = ?
            AND speed IS NOT NULL
        """
        telemetry = self.database.fetch_dataframe(query, {"session_key": session_key, "driver_number": driver_number})
        
        if telemetry.empty:
            return {}
        
        # Calculate braking percentage
        braking_time = len(telemetry[telemetry["brake"] > 0]) if "brake" in telemetry.columns else 0
        total_time = len(telemetry)
        braking_pct = (braking_time / total_time * 100) if total_time > 0 else 0
        
        return {
            "avg_speed": telemetry["speed"].mean(),
            "max_speed": telemetry["speed"].max(),
            "avg_throttle": telemetry["throttle"].mean() if "throttle" in telemetry.columns else None,
            "time_spent_braking_pct": braking_pct
        }
    
    def _calculate_tire_performance(self, session_key: int, driver_number: int) -> Dict[str, Any]:
        """Calculate tire performance metrics."""
        query = """
            SELECT compound, stint_number 
            FROM raw_stints 
            WHERE session_key = ? AND driver_number = ?
        """
        stints = self.database.fetch_dataframe(query, {"session_key": session_key, "driver_number": driver_number})
        
        if stints.empty:
            return {"compounds_used": [], "stint_count": 0}
        
        return {
            "compounds_used": stints["compound"].unique().tolist(),
            "stint_count": len(stints)
        }