"""
AWS Glue ETL Job: Raw F1 Data Processor

This job processes raw F1 data from the OpenF1 API into structured format
ready for further transformation into Iceberg tables.

Key Functions:
- Reads raw JSON data from S3
- Validates and cleans data
- Partitions by year and endpoint
- Outputs to processed-data bucket location
"""

import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DATABASE_NAME', 
    'DATA_LAKE_BUCKET',
    'ENVIRONMENT'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glue_context = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "true").getOrCreate()
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Constants
DATABASE_NAME = args['DATABASE_NAME']
DATA_LAKE_BUCKET = args['DATA_LAKE_BUCKET']
ENVIRONMENT = args['ENVIRONMENT']

def read_raw_data(endpoint: str, year: int) -> DataFrame:
    """Read raw F1 data from S3 for a specific endpoint and year."""
    
    input_path = f"s3://{DATA_LAKE_BUCKET}/raw-data/year={year}/endpoint={endpoint}/"
    
    try:
        logger.info(f"Reading raw data from: {input_path}")
        
        # Read JSON files with error handling
        df = spark.read.option("multiLine", "true").json(input_path)
        
        if df.count() == 0:
            logger.warning(f"No data found at {input_path}")
            return None
            
        # Add metadata columns
        df = df.withColumn("extraction_year", lit(year)) \
              .withColumn("endpoint", lit(endpoint)) \
              .withColumn("processing_timestamp", current_timestamp())
        
        logger.info(f"Successfully read {df.count()} records from {endpoint} for {year}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading data from {input_path}: {str(e)}")
        return None

def process_meetings_data(df: DataFrame) -> DataFrame:
    """Process F1 meetings data with proper data types."""
    
    logger.info("Processing meetings data...")
    
    # Define schema and clean data
    processed_df = df.select(
        col("meeting_key").cast("bigint"),
        col("meeting_name").cast("string"),
        col("meeting_official_name").cast("string"),
        col("location").cast("string"),
        col("country_key").cast("bigint"),
        col("country_code").cast("string"),
        col("country_name").cast("string"),
        col("circuit_key").cast("bigint"),
        col("circuit_short_name").cast("string"),
        to_timestamp(col("date_start")).alias("date_start"),
        col("year").cast("int"),
        col("gmt_offset").cast("string"),
        current_timestamp().alias("processing_timestamp")
    ).filter(col("meeting_key").isNotNull())
    
    return processed_df

def process_sessions_data(df: DataFrame) -> DataFrame:
    """Process F1 sessions data with proper data types."""
    
    logger.info("Processing sessions data...")
    
    processed_df = df.select(
        col("session_key").cast("bigint"),
        col("session_name").cast("string"),
        col("session_type").cast("string"),
        to_timestamp(col("date_start")).alias("date_start"),
        to_timestamp(col("date_end")).alias("date_end"),
        col("gmt_offset").cast("string"),
        col("meeting_key").cast("bigint"),
        current_timestamp().alias("processing_timestamp")
    ).filter(col("session_key").isNotNull())
    
    return processed_df

def process_drivers_data(df: DataFrame) -> DataFrame:
    """Process F1 drivers data with proper data types."""
    
    logger.info("Processing drivers data...")
    
    processed_df = df.select(
        col("driver_number").cast("int"),
        col("broadcast_name").cast("string"),
        col("full_name").cast("string"),
        col("name_acronym").cast("string"),
        col("team_name").cast("string"),
        col("team_colour").cast("string"),
        col("first_name").cast("string"),
        col("last_name").cast("string"),
        col("headshot_url").cast("string"),
        col("country_code").cast("string"),
        col("session_key").cast("bigint"),
        current_timestamp().alias("processing_timestamp")
    ).filter(col("driver_number").isNotNull())
    
    return processed_df

def process_laps_data(df: DataFrame) -> DataFrame:
    """Process F1 laps data with proper data types and performance optimizations."""
    
    logger.info("Processing laps data...")
    
    processed_df = df.select(
        to_date(col("date_start")).alias("date_start"),
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("lap_number").cast("int"),
        col("lap_duration").cast("double"),
        col("lap_time").cast("double"),
        col("i1_speed").cast("double"),
        col("i2_speed").cast("double"),
        col("st_speed").cast("double"),
        col("is_personal_best").cast("boolean"),
        col("compound").cast("string"),
        col("tyre_life").cast("int"),
        col("fresh_tyre").cast("boolean"),
        current_timestamp().alias("processing_timestamp")
    ).filter(
        col("session_key").isNotNull() & 
        col("driver_number").isNotNull() &
        col("lap_number").isNotNull()
    )
    
    return processed_df

def process_positions_data(df: DataFrame) -> DataFrame:
    """Process F1 positions data with proper data types."""
    
    logger.info("Processing positions data...")
    
    processed_df = df.select(
        to_date(col("date")).alias("date"),
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("position").cast("int"),
        current_timestamp().alias("processing_timestamp")
    ).filter(
        col("session_key").isNotNull() & 
        col("driver_number").isNotNull()
    )
    
    return processed_df

def write_processed_data(df: DataFrame, endpoint: str, year: int) -> None:
    """Write processed data to S3 in Parquet format with partitioning."""
    
    output_path = f"s3://{DATA_LAKE_BUCKET}/processed-data/{endpoint}/year={year}/"
    
    try:
        logger.info(f"Writing {df.count()} records to: {output_path}")
        
        # Configure write options for performance
        df.coalesce(4) \
          .write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(output_path)
        
        logger.info(f"Successfully wrote processed data for {endpoint} {year}")
        
    except Exception as e:
        logger.error(f"Error writing processed data to {output_path}: {str(e)}")
        raise

def main():
    """Main processing function."""
    
    logger.info(f"Starting raw data processing job for environment: {ENVIRONMENT}")
    
    # Define endpoints and years to process
    endpoints_processors = {
        'meetings': process_meetings_data,
        'sessions': process_sessions_data,
        'drivers': process_drivers_data,
        'laps': process_laps_data,
        'position': process_positions_data
    }
    
    years = [2019, 2020, 2021, 2022, 2023]
    
    processing_summary = []
    
    for endpoint, processor_func in endpoints_processors.items():
        for year in years:
            try:
                logger.info(f"Processing {endpoint} data for year {year}")
                
                # Read raw data
                raw_df = read_raw_data(endpoint, year)
                if raw_df is None:
                    logger.warning(f"Skipping {endpoint} {year} - no data found")
                    continue
                
                # Process data using endpoint-specific processor
                processed_df = processor_func(raw_df)
                
                # Write processed data
                write_processed_data(processed_df, endpoint, year)
                
                processing_summary.append({
                    'endpoint': endpoint,
                    'year': year,
                    'records_processed': processed_df.count(),
                    'status': 'SUCCESS'
                })
                
            except Exception as e:
                logger.error(f"Error processing {endpoint} {year}: {str(e)}")
                processing_summary.append({
                    'endpoint': endpoint,
                    'year': year,
                    'records_processed': 0,
                    'status': f'ERROR: {str(e)}'
                })
                continue
    
    # Log processing summary
    logger.info("Processing Summary:")
    for summary in processing_summary:
        logger.info(f"  {summary['endpoint']} {summary['year']}: {summary['records_processed']} records - {summary['status']}")
    
    logger.info("Raw data processing job completed")

if __name__ == "__main__":
    main()
    job.commit()