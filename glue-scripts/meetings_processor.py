"""
AWS Glue ETL Job: F1 Meetings Iceberg Processor

This job processes F1 meetings data into Apache Iceberg format tables
for high-performance analytics in Athena.

Key Functions:
- Reads processed meetings data from S3
- Creates/updates Iceberg table with ACID transactions
- Handles schema evolution and data quality checks
- Optimizes table layout for query performance
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

# Initialize Spark with Iceberg support
sc = SparkContext()
glue_context = GlueContext(sc)

# Configure Spark for Iceberg
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['DATA_LAKE_BUCKET']}/iceberg-tables/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Constants
DATABASE_NAME = args['DATABASE_NAME']
DATA_LAKE_BUCKET = args['DATA_LAKE_BUCKET']
ENVIRONMENT = args['ENVIRONMENT']
TABLE_NAME = f"glue_catalog.{DATABASE_NAME}.f1_meetings"

def read_processed_meetings_data() -> DataFrame:
    """Read processed F1 meetings data from S3."""
    
    input_path = f"s3://{DATA_LAKE_BUCKET}/processed-data/meetings/"
    
    try:
        logger.info(f"Reading processed meetings data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        
        if df.count() == 0:
            logger.warning("No meetings data found")
            return None
            
        logger.info(f"Successfully read {df.count()} meeting records")
        return df
        
    except Exception as e:
        logger.error(f"Error reading meetings data: {str(e)}")
        return None

def create_meetings_iceberg_table() -> None:
    """Create the F1 meetings Iceberg table if it doesn't exist."""
    
    try:
        # Check if table exists
        existing_tables = spark.sql(f"SHOW TABLES IN glue_catalog.{DATABASE_NAME}").collect()
        table_exists = any(row['tableName'] == 'f1_meetings' for row in existing_tables)
        
        if not table_exists:
            logger.info("Creating F1 meetings Iceberg table...")
            
            create_table_sql = f"""
            CREATE TABLE {TABLE_NAME} (
                meeting_key bigint,
                meeting_name string,
                meeting_official_name string,
                location string,
                country_key bigint,
                country_code string,
                country_name string,
                circuit_key bigint,
                circuit_short_name string,
                date_start timestamp,
                year int,
                gmt_offset string,
                processing_timestamp timestamp
            ) USING ICEBERG
            PARTITIONED BY (year)
            TBLPROPERTIES (
                'format-version'='2',
                'write.parquet.compression-codec'='snappy',
                'write.metadata.delete-after-commit.enabled'='true',
                'write.metadata.previous-versions-max'='5'
            )
            """
            
            spark.sql(create_table_sql)
            logger.info("F1 meetings Iceberg table created successfully")
        else:
            logger.info("F1 meetings Iceberg table already exists")
            
    except Exception as e:
        logger.error(f"Error creating meetings Iceberg table: {str(e)}")
        raise

def validate_meetings_data(df: DataFrame) -> DataFrame:
    """Validate and clean F1 meetings data."""
    
    logger.info("Validating meetings data...")
    
    # Data quality checks
    initial_count = df.count()
    
    # Remove duplicates based on meeting_key and year
    df_clean = df.dropDuplicates(['meeting_key', 'year'])
    
    # Filter out invalid records
    df_clean = df_clean.filter(
        col("meeting_key").isNotNull() &
        col("year").isNotNull() &
        col("year").between(2018, 2030) &  # Reasonable year range
        col("meeting_name").isNotNull()
    )
    
    # Add data quality metrics
    df_clean = df_clean.withColumn("data_quality_score", 
        when(col("meeting_official_name").isNotNull(), 1.0)
        .when(col("location").isNotNull(), 0.8)
        .otherwise(0.6)
    )
    
    final_count = df_clean.count()
    logger.info(f"Data validation: {initial_count} -> {final_count} records (removed {initial_count - final_count} invalid records)")
    
    return df_clean

def upsert_meetings_data(df: DataFrame) -> None:
    """Upsert F1 meetings data into Iceberg table using merge operation."""
    
    try:
        logger.info(f"Upserting {df.count()} meeting records into Iceberg table...")
        
        # Create a temporary view for the merge operation
        df.createOrReplaceTempView("meetings_updates")
        
        # Perform merge operation (upsert)
        merge_sql = f"""
        MERGE INTO {TABLE_NAME} target
        USING meetings_updates source
        ON target.meeting_key = source.meeting_key AND target.year = source.year
        WHEN MATCHED THEN 
            UPDATE SET 
                meeting_name = source.meeting_name,
                meeting_official_name = source.meeting_official_name,
                location = source.location,
                country_key = source.country_key,
                country_code = source.country_code,
                country_name = source.country_name,
                circuit_key = source.circuit_key,
                circuit_short_name = source.circuit_short_name,
                date_start = source.date_start,
                gmt_offset = source.gmt_offset,
                processing_timestamp = source.processing_timestamp
        WHEN NOT MATCHED THEN 
            INSERT (
                meeting_key, meeting_name, meeting_official_name, location,
                country_key, country_code, country_name, circuit_key,
                circuit_short_name, date_start, year, gmt_offset, processing_timestamp
            ) VALUES (
                source.meeting_key, source.meeting_name, source.meeting_official_name, source.location,
                source.country_key, source.country_code, source.country_name, source.circuit_key,
                source.circuit_short_name, source.date_start, source.year, source.gmt_offset, source.processing_timestamp
            )
        """
        
        spark.sql(merge_sql)
        
        # Optimize table layout for better query performance
        optimize_sql = f"OPTIMIZE {TABLE_NAME}"
        spark.sql(optimize_sql)
        
        logger.info("Meetings data upserted and table optimized successfully")
        
    except Exception as e:
        logger.error(f"Error upserting meetings data: {str(e)}")
        raise

def analyze_table_statistics() -> None:
    """Analyze and log table statistics for monitoring."""
    
    try:
        logger.info("Analyzing table statistics...")
        
        # Get row count by year
        stats_df = spark.sql(f"""
            SELECT 
                year,
                COUNT(*) as meeting_count,
                COUNT(DISTINCT country_name) as unique_countries,
                COUNT(DISTINCT circuit_short_name) as unique_circuits
            FROM {TABLE_NAME}
            GROUP BY year
            ORDER BY year DESC
        """)
        
        logger.info("Table Statistics:")
        for row in stats_df.collect():
            logger.info(f"  Year {row['year']}: {row['meeting_count']} meetings, "
                       f"{row['unique_countries']} countries, {row['unique_circuits']} circuits")
        
        # Get total table size and file count
        table_info = spark.sql(f"DESCRIBE EXTENDED {TABLE_NAME}").collect()
        logger.info("Table metadata updated successfully")
        
    except Exception as e:
        logger.error(f"Error analyzing table statistics: {str(e)}")

def main():
    """Main processing function."""
    
    logger.info(f"Starting F1 meetings Iceberg processing job for environment: {ENVIRONMENT}")
    
    try:
        # Read processed meetings data
        meetings_df = read_processed_meetings_data()
        if meetings_df is None:
            logger.warning("No meetings data to process, exiting")
            return
        
        # Create Iceberg table if needed
        create_meetings_iceberg_table()
        
        # Validate and clean data
        clean_meetings_df = validate_meetings_data(meetings_df)
        
        # Upsert data into Iceberg table
        upsert_meetings_data(clean_meetings_df)
        
        # Analyze table statistics
        analyze_table_statistics()
        
        logger.info("F1 meetings Iceberg processing job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    job.commit()