"""Data extraction orchestrator for OpenF1 API."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

from .openf1_client import OpenF1Client
from ..cloud_swap import CloudProvider
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class DataExtractor:
    """Orchestrates data extraction from OpenF1 API to cloud storage."""

    def __init__(self, settings: Settings, cloud_provider: CloudProvider):
        """Initialize the data extractor.
        
        Args:
            settings: Application settings
            cloud_provider: Cloud provider instance
        """
        self.settings = settings
        self.cloud_provider = cloud_provider
        self.storage = cloud_provider.get_storage_provider()
        self.database = cloud_provider.get_database_provider()
        
        # Initialize OpenF1 client
        self.openf1_client = OpenF1Client(
            base_url=settings.openf1.base_url,
            rate_limit_delay=settings.openf1.rate_limit_delay,
            max_retries=settings.openf1.max_retries,
            timeout=settings.openf1.timeout
        )

    def extract_year_data(self, year: int, save_raw: bool = True, 
                         save_to_db: bool = True) -> Dict[str, int]:
        """Extract all data for a specific year.
        
        Args:
            year: Year to extract data for
            save_raw: Whether to save raw data to storage
            save_to_db: Whether to save data to database
            
        Returns:
            Dictionary with extraction statistics
        """
        logger.info(f"Starting data extraction for year {year}")
        
        stats = {
            "year": year,
            "endpoints_processed": 0,
            "total_records": 0,
            "files_saved": 0,
            "errors": 0
        }
        
        extraction_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        try:
            for endpoint_name, dataframe in self.openf1_client.get_all_data_for_year(year):
                try:
                    record_count = len(dataframe)
                    logger.info(f"Extracted {record_count} records from {endpoint_name} for year {year}")
                    
                    # Save raw data to storage if requested
                    if save_raw and record_count > 0:
                        raw_path = f"raw_data/year={year}/endpoint={endpoint_name}/data_{extraction_timestamp}.parquet"
                        success = self.storage.upload_dataframe(dataframe, raw_path, format="parquet")
                        if success:
                            stats["files_saved"] += 1
                            logger.debug(f"Saved raw data to {raw_path}")
                        else:
                            logger.error(f"Failed to save raw data to {raw_path}")
                            stats["errors"] += 1
                    
                    # Save to database if requested
                    if save_to_db and record_count > 0:
                        table_name = f"raw_{endpoint_name}"
                        success = self._save_to_database(dataframe, table_name)
                        if not success:
                            logger.error(f"Failed to save {endpoint_name} data to database")
                            stats["errors"] += 1
                    
                    stats["endpoints_processed"] += 1
                    stats["total_records"] += record_count
                    
                except Exception as e:
                    logger.error(f"Error processing {endpoint_name} for year {year}: {e}")
                    stats["errors"] += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error during data extraction for year {year}: {e}")
            stats["errors"] += 1
        
        logger.info(f"Completed extraction for year {year}. Stats: {stats}")
        return stats

    def extract_multiple_years(self, years: List[int], save_raw: bool = True,
                              save_to_db: bool = True) -> Dict[str, Any]:
        """Extract data for multiple years.
        
        Args:
            years: List of years to extract data for
            save_raw: Whether to save raw data to storage
            save_to_db: Whether to save data to database
            
        Returns:
            Dictionary with overall extraction statistics
        """
        logger.info(f"Starting multi-year data extraction for years: {years}")
        
        overall_stats = {
            "years_processed": [],
            "total_endpoints_processed": 0,
            "total_records": 0,
            "total_files_saved": 0,
            "total_errors": 0,
            "year_stats": {}
        }
        
        for year in years:
            year_stats = self.extract_year_data(year, save_raw, save_to_db)
            
            overall_stats["years_processed"].append(year)
            overall_stats["total_endpoints_processed"] += year_stats["endpoints_processed"]
            overall_stats["total_records"] += year_stats["total_records"]
            overall_stats["total_files_saved"] += year_stats["files_saved"]
            overall_stats["total_errors"] += year_stats["errors"]
            overall_stats["year_stats"][year] = year_stats
        
        logger.info(f"Completed multi-year extraction. Overall stats: {overall_stats}")
        return overall_stats

    def extract_incremental_data(self, endpoint_name: str, 
                                params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Extract incremental data from a specific endpoint.
        
        Args:
            endpoint_name: Name of the endpoint to query
            params: Query parameters
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Extracting incremental data from {endpoint_name}")
        
        try:
            dataframe = self.openf1_client.get_data(endpoint_name, params)
            logger.info(f"Extracted {len(dataframe)} records from {endpoint_name}")
            return dataframe
        except Exception as e:
            logger.error(f"Error extracting incremental data from {endpoint_name}: {e}")
            return pd.DataFrame()

    def _save_to_database(self, dataframe: pd.DataFrame, table_name: str) -> bool:
        """Save dataframe to database.
        
        Args:
            dataframe: DataFrame to save
            table_name: Name of the database table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure we have a connection
            self.database.connect()
            
            # Save the dataframe
            success = self.database.insert_dataframe(dataframe, table_name, if_exists="append")
            
            if success:
                logger.debug(f"Successfully saved {len(dataframe)} records to {table_name}")
            else:
                logger.error(f"Failed to save dataframe to {table_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving to database table {table_name}: {e}")
            return False

    def create_raw_data_tables(self) -> Dict[str, bool]:
        """Create database tables for raw data storage.
        
        Returns:
            Dictionary mapping table names to creation success status
        """
        logger.info("Creating raw data tables")
        
        # Define schemas for raw data tables based on OpenF1 API structure
        schemas = {
            "raw_meetings": {
                "meeting_key": "INTEGER",
                "meeting_name": "TEXT",
                "meeting_official_name": "TEXT", 
                "location": "TEXT",
                "country_name": "TEXT",
                "country_code": "TEXT",
                "country_key": "INTEGER",
                "circuit_key": "INTEGER",
                "circuit_short_name": "TEXT",
                "date_start": "TIMESTAMP",
                "gmt_offset": "TEXT",
                "year": "INTEGER",
                "_extracted_at": "TIMESTAMP",
                "_endpoint": "TEXT"
            },
            "raw_sessions": {
                "session_key": "INTEGER",
                "session_name": "TEXT",
                "session_type": "TEXT",
                "meeting_key": "INTEGER",
                "location": "TEXT",
                "country_name": "TEXT",
                "country_code": "TEXT",
                "country_key": "INTEGER",
                "circuit_key": "INTEGER",
                "circuit_short_name": "TEXT",
                "date_start": "TIMESTAMP",
                "date_end": "TIMESTAMP",
                "gmt_offset": "TEXT",
                "year": "INTEGER",
                "_extracted_at": "TIMESTAMP",
                "_endpoint": "TEXT"
            },
            "raw_drivers": {
                "session_key": "INTEGER",
                "meeting_key": "INTEGER",
                "driver_number": "INTEGER",
                "broadcast_name": "TEXT",
                "country_code": "TEXT",
                "first_name": "TEXT",
                "full_name": "TEXT",
                "headshot_url": "TEXT",
                "last_name": "TEXT",
                "name_acronym": "TEXT",
                "team_colour": "TEXT",
                "team_name": "TEXT",
                "_extracted_at": "TIMESTAMP",
                "_endpoint": "TEXT",
                "year": "INTEGER"
            },
            "raw_laps": {
                "session_key": "INTEGER",
                "meeting_key": "INTEGER",
                "driver_number": "INTEGER",
                "date_start": "TIMESTAMP",
                "duration_sector_1": "REAL",
                "duration_sector_2": "REAL", 
                "duration_sector_3": "REAL",
                "i1_speed": "INTEGER",
                "i2_speed": "INTEGER",
                "is_pit_out_lap": "BOOLEAN",
                "lap_duration": "REAL",
                "lap_number": "INTEGER",
                "st_speed": "INTEGER",
                "_extracted_at": "TIMESTAMP",
                "_endpoint": "TEXT",
                "year": "INTEGER"
            }
            # Add more schemas as needed for other endpoints
        }
        
        results = {}
        for table_name, schema in schemas.items():
            try:
                success = self.database.create_table(table_name, schema)
                results[table_name] = success
                if success:
                    logger.info(f"Created table {table_name}")
                else:
                    logger.error(f"Failed to create table {table_name}")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {e}")
                results[table_name] = False
        
        return results

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on extraction components.
        
        Returns:
            Dictionary with health check results
        """
        logger.info("Performing extraction health check")
        
        health = {
            "openf1_api": {},
            "storage": {},
            "database": {},
            "overall_status": "unknown"
        }
        
        # Check OpenF1 API
        try:
            health["openf1_api"] = self.openf1_client.health_check()
        except Exception as e:
            health["openf1_api"] = {"status": "error", "error": str(e)}
        
        # Check cloud provider
        try:
            cloud_health = self.cloud_provider.health_check()
            health["storage"] = {"status": "healthy" if cloud_health.get("storage", False) else "unhealthy"}
            health["database"] = {"status": "healthy" if cloud_health.get("database", False) else "unhealthy"}
        except Exception as e:
            health["storage"] = {"status": "error", "error": str(e)}
            health["database"] = {"status": "error", "error": str(e)}
        
        # Determine overall status
        all_healthy = (
            health["openf1_api"].get("status") == "healthy" and
            health["storage"].get("status") == "healthy" and
            health["database"].get("status") == "healthy"
        )
        
        health["overall_status"] = "healthy" if all_healthy else "unhealthy"
        
        return health