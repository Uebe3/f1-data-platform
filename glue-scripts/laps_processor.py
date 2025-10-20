"""
AWS Glue ETL Job: F1 Laps Iceberg Processor

This job processes F1 laps data into Apache Iceberg format tables
with optimal partitioning for high-performance analytics.

Key Functions:
- Reads processed laps data from S3
- Creates partitioned Iceberg table by date for performance
- Handles large datasets with proper optimization
- Implements data quality checks and deduplication
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

# Initialize Spark with Iceberg support and performance optimizations
sc = SparkContext()
glue_context = GlueContext(sc)

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['DATA_LAKE_BUCKET']}/iceberg-tables/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .getOrCreate()

job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Constants
DATABASE_NAME = args['DATABASE_NAME']
DATA_LAKE_BUCKET = args['DATA_LAKE_BUCKET']
ENVIRONMENT = args['ENVIRONMENT']
TABLE_NAME = f"glue_catalog.{DATABASE_NAME}.f1_laps"

def read_processed_laps_data() -> DataFrame:
    """Read processed F1 laps data from S3."""
    
    input_path = f"s3://{DATA_LAKE_BUCKET}/processed-data/laps/"
    
    try:
        logger.info(f"Reading processed laps data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        if df.count() == 0:
            logger.warning("No laps data found")
            return None
            
        logger.info(f"Successfully read laps data with {df.count()} records")
        return df
        
    except Exception as e:
        logger.error(f"Error reading laps data: {str(e)}")
        return None

def create_laps_iceberg_table() -> None:
    """Create the F1 laps Iceberg table with optimal partitioning."""
    
    try:
        existing_tables = spark.sql(f"SHOW TABLES IN glue_catalog.{DATABASE_NAME}").collect()
        table_exists = any(row['tableName'] == 'f1_laps' for row in existing_tables)
        
        if not table_exists:
            logger.info("Creating F1 laps Iceberg table with date partitioning...")
            
            create_table_sql = f"""
            CREATE TABLE {TABLE_NAME} (
                session_key bigint,
                driver_number int,
                lap_number int,
                lap_duration double,
                lap_time double,
                i1_speed double,
                i2_speed double,
                st_speed double,
                is_personal_best boolean,
                compound string,
                tyre_life int,
                fresh_tyre boolean,
                processing_timestamp timestamp,
                date_start date
            ) USING ICEBERG
            PARTITIONED BY (date_start)
            TBLPROPERTIES (
                'format-version'='2',
                'write.parquet.compression-codec'='snappy',
                'write.metadata.delete-after-commit.enabled'='true',
                'write.metadata.previous-versions-max'='3',
                'write.target-file-size-bytes'='134217728',
                'write.parquet.row-group-size-bytes'='8388608'
            )
            """
            
            spark.sql(create_table_sql)
            logger.info("F1 laps Iceberg table created successfully")
        else:
            logger.info("F1 laps Iceberg table already exists")
            
    except Exception as e:
        logger.error(f"Error creating laps Iceberg table: {str(e)}")
        raise

def validate_and_clean_laps_data(df: DataFrame) -> DataFrame:
    """Validate and clean F1 laps data with comprehensive quality checks."""
    
    logger.info("Validating and cleaning laps data...")
    
    initial_count = df.count()
    
    # Remove duplicates based on unique lap identifier
    df_clean = df.dropDuplicates(['session_key', 'driver_number', 'lap_number'])
    
    # Data quality filters
    df_clean = df_clean.filter(
        # Basic not null checks
        col("session_key").isNotNull() &
        col("driver_number").isNotNull() &
        col("lap_number").isNotNull() &
        col("date_start").isNotNull() &
        
        # Reasonable value ranges
        col("lap_number") > 0 &
        col("lap_number") < 200 &  # Max laps in any session
        col("driver_number").between(1, 99) &
        
        # Lap time validations (if present)
        (col("lap_time").isNull() | (col("lap_time") > 0)) &
        (col("lap_duration").isNull() | (col("lap_duration") > 0)) &
        
        # Speed validations (if present)
        (col("i1_speed").isNull() | (col("i1_speed") > 0)) &
        (col("i2_speed").isNull() | (col("i2_speed") > 0)) &
        (col("st_speed").isNull() | (col("st_speed") > 0)) &
        
        # Tyre life validations
        (col("tyre_life").isNull() | (col("tyre_life") >= 0))
    )
    
    # Add data quality score based on completeness
    df_clean = df_clean.withColumn("data_completeness_score",
        (
            when(col("lap_time").isNotNull(), 0.3).otherwise(0.0) +
            when(col("i1_speed").isNotNull(), 0.2).otherwise(0.0) +
            when(col("i2_speed").isNotNull(), 0.2).otherwise(0.0) +
            when(col("st_speed").isNotNull(), 0.2).otherwise(0.0) +
            when(col("compound").isNotNull(), 0.1).otherwise(0.0)
        )
    )
    
    final_count = df_clean.count()
    logger.info(f"Data validation: {initial_count} -> {final_count} records "
               f"(removed {initial_count - final_count} invalid records)")
    
    return df_clean

def process_laps_in_batches(df: DataFrame) -> None:
    """Process laps data in batches by date partition for better performance."""
    
    try:
        # Get distinct dates for batch processing
        dates_df = df.select("date_start").distinct().orderBy("date_start")
        date_partitions = [row["date_start"] for row in dates_df.collect()]
        
        logger.info(f"Processing {len(date_partitions)} date partitions")
        
        for date_partition in date_partitions:
            try:
                # Process one date partition at a time
                partition_df = df.filter(col("date_start") == date_partition)
                partition_count = partition_df.count()
                
                logger.info(f"Processing {partition_count} laps for date: {date_partition}")
                
                # Create temporary view for merge operation
                partition_df.createOrReplaceTempView("laps_updates")
                
                # Perform merge operation for this partition
                merge_sql = f"""
                MERGE INTO {TABLE_NAME} target
                USING laps_updates source
                ON target.session_key = source.session_key 
                   AND target.driver_number = source.driver_number 
                   AND target.lap_number = source.lap_number
                   AND target.date_start = source.date_start
                WHEN MATCHED THEN 
                    UPDATE SET 
                        lap_duration = source.lap_duration,
                        lap_time = source.lap_time,
                        i1_speed = source.i1_speed,
                        i2_speed = source.i2_speed,
                        st_speed = source.st_speed,
                        is_personal_best = source.is_personal_best,
                        compound = source.compound,
                        tyre_life = source.tyre_life,
                        fresh_tyre = source.fresh_tyre,
                        processing_timestamp = source.processing_timestamp
                WHEN NOT MATCHED THEN 
                    INSERT (
                        session_key, driver_number, lap_number, lap_duration, lap_time,
                        i1_speed, i2_speed, st_speed, is_personal_best, compound,
                        tyre_life, fresh_tyre, processing_timestamp, date_start
                    ) VALUES (
                        source.session_key, source.driver_number, source.lap_number, 
                        source.lap_duration, source.lap_time, source.i1_speed, 
                        source.i2_speed, source.st_speed, source.is_personal_best, 
                        source.compound, source.tyre_life, source.fresh_tyre, 
                        source.processing_timestamp, source.date_start
                    )
                """
                
                spark.sql(merge_sql)
                
                logger.info(f"Successfully processed partition for date: {date_partition}")
                
            except Exception as e:
                logger.error(f"Error processing partition {date_partition}: {str(e)}")
                continue
        
        # Optimize table after all batches
        logger.info("Optimizing laps table layout...")
        optimize_sql = f"OPTIMIZE {TABLE_NAME}"
        spark.sql(optimize_sql)
        
        logger.info("All laps data processed and table optimized successfully")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

def analyze_laps_statistics() -> None:
    """Analyze and log laps table statistics."""
    
    try:
        logger.info("Analyzing laps table statistics...")
        
        # Get statistics by date partition
        stats_df = spark.sql(f"""
            SELECT 
                date_start,
                COUNT(*) as total_laps,
                COUNT(DISTINCT session_key) as unique_sessions,
                COUNT(DISTINCT driver_number) as unique_drivers,
                AVG(data_completeness_score) as avg_completeness_score,
                COUNT(CASE WHEN lap_time IS NOT NULL THEN 1 END) as laps_with_times
            FROM {TABLE_NAME}
            GROUP BY date_start
            ORDER BY date_start DESC
            LIMIT 10
        """)
        
        logger.info("Recent Laps Statistics (Last 10 dates):")
        for row in stats_df.collect():
            logger.info(f"  {row['date_start']}: {row['total_laps']} laps, "
                       f"{row['unique_sessions']} sessions, {row['unique_drivers']} drivers, "
                       f"completeness: {row['avg_completeness_score']:.2f}")
        
        # Get overall table statistics
        total_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as total_laps,
                MIN(date_start) as earliest_date,
                MAX(date_start) as latest_date,
                COUNT(DISTINCT date_start) as total_dates
            FROM {TABLE_NAME}
        """).collect()[0]
        
        logger.info(f"Total table statistics: {total_stats['total_laps']} laps, "
                   f"from {total_stats['earliest_date']} to {total_stats['latest_date']}, "
                   f"across {total_stats['total_dates']} dates")
        
    except Exception as e:
        logger.error(f"Error analyzing table statistics: {str(e)}")

def main():
    """Main processing function."""
    
    logger.info(f"Starting F1 laps Iceberg processing job for environment: {ENVIRONMENT}")
    
    try:
        # Read processed laps data
        laps_df = read_processed_laps_data()
        if laps_df is None:
            logger.warning("No laps data to process, exiting")
            return
        
        # Create Iceberg table if needed
        create_laps_iceberg_table()
        
        # Validate and clean data
        clean_laps_df = validate_and_clean_laps_data(laps_df)
        
        # Process data in batches by date partition
        process_laps_in_batches(clean_laps_df)
        
        # Analyze table statistics
        analyze_laps_statistics()
        
        logger.info("F1 laps Iceberg processing job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    job.commit()