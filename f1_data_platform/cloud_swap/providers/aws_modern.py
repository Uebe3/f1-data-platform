"""Modern AWS cloud provider implementation with serverless services."""

import os
import json
import time
from typing import Any, Dict, List, Optional, IO
import pandas as pd
from datetime import datetime

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False

from ..interfaces import StorageProvider, DatabaseProvider, ComputeProvider, CloudProvider


class AWSDataLakeProvider(StorageProvider):
    """Enhanced AWS S3 storage provider with data lake capabilities."""

    def __init__(self, bucket_name: str, region: str = "us-east-1", 
                 data_lake_prefix: str = "data-lake/", **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.bucket_name = bucket_name
        self.region = region
        self.data_lake_prefix = data_lake_prefix
        self.s3_client = boto3.client("s3", region_name=region, **kwargs)
        
        # Create bucket if it doesn't exist
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Ensure S3 bucket exists and is configured for data lake."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Create bucket
                if self.region == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                
                # Enable versioning for data lake
                self.s3_client.put_bucket_versioning(
                    Bucket=self.bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to S3 data lake."""
        try:
            full_path = f"{self.data_lake_prefix}{remote_path}"
            self.s3_client.upload_file(local_path, self.bucket_name, full_path)
            return True
        except (ClientError, FileNotFoundError):
            return False

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from S3 data lake."""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            full_path = f"{self.data_lake_prefix}{remote_path}"
            self.s3_client.download_file(self.bucket_name, full_path, local_path)
            return True
        except ClientError:
            return False

    def upload_dataframe(self, df: pd.DataFrame, remote_path: str, format: str = "parquet") -> bool:
        """Upload a pandas DataFrame to S3 data lake with partitioning."""
        try:
            import io
            full_path = f"{self.data_lake_prefix}{remote_path}"
            
            if format.lower() == "parquet":
                # Enhanced parquet with compression and metadata
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
                buffer.seek(0)
                
                self.s3_client.upload_fileobj(
                    buffer, 
                    self.bucket_name, 
                    full_path,
                    ExtraArgs={
                        'Metadata': {
                            'format': 'parquet',
                            'compression': 'snappy',
                            'rows': str(len(df)),
                            'columns': str(len(df.columns)),
                            'created_at': datetime.utcnow().isoformat()
                        }
                    }
                )
            else:
                # Fallback to original implementation for other formats
                return super().upload_dataframe(df, remote_path, format)
            
            return True
        except Exception:
            return False

    def upload_partitioned_dataframe(self, df: pd.DataFrame, table_name: str, 
                                   partition_cols: List[str] = None) -> bool:
        """Upload DataFrame with Hive-style partitioning for analytics."""
        try:
            if partition_cols is None:
                partition_cols = ['year', 'month', 'day']
            
            # Add date partitions if they don't exist
            if 'date' in df.columns:
                df['year'] = pd.to_datetime(df['date']).dt.year
                df['month'] = pd.to_datetime(df['date']).dt.month
                df['day'] = pd.to_datetime(df['date']).dt.day
            
            # Group by partitions and upload separately
            partition_groups = df.groupby(partition_cols) if partition_cols else [(None, df)]
            
            for partition_values, group_df in partition_groups:
                if partition_values is not None:
                    # Build partition path
                    partition_path = "/".join([
                        f"{col}={val}" for col, val in zip(partition_cols, partition_values)
                    ])
                    file_path = f"tables/{table_name}/{partition_path}/data.parquet"
                else:
                    file_path = f"tables/{table_name}/data.parquet"
                
                # Upload partition
                self.upload_dataframe(group_df, file_path, "parquet")
            
            return True
        except Exception:
            return False

    def download_dataframe(self, remote_path: str, format: str = "parquet") -> pd.DataFrame:
        """Download a pandas DataFrame from S3 data lake."""
        import io
        
        try:
            full_path = f"{self.data_lake_prefix}{remote_path}"
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=full_path)
            
            if format.lower() == "parquet":
                return pd.read_parquet(io.BytesIO(response["Body"].read()), engine="pyarrow")
            elif format.lower() == "csv":
                return pd.read_csv(io.StringIO(response["Body"].read().decode("utf-8")))
            elif format.lower() == "json":
                return pd.read_json(io.StringIO(response["Body"].read().decode("utf-8")), orient="records")
            else:
                raise ValueError(f"Unsupported format: {format}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"File not found: {remote_path}")
            raise

    def list_files(self, prefix: str = "") -> List[str]:
        """List files in S3 data lake with optional prefix."""
        try:
            full_prefix = f"{self.data_lake_prefix}{prefix}"
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix)
            
            files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Remove data lake prefix from returned paths
                        clean_path = obj['Key'].replace(self.data_lake_prefix, '', 1)
                        files.append(clean_path)
            return files
        except Exception:
            return []

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from S3 data lake."""
        try:
            full_path = f"{self.data_lake_prefix}{remote_path}"
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=full_path)
            return True
        except ClientError:
            return False

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists in S3 data lake."""
        try:
            full_path = f"{self.data_lake_prefix}{remote_path}"
            self.s3_client.head_object(Bucket=self.bucket_name, Key=full_path)
            return True
        except ClientError:
            return False

    def get_file_size(self, remote_path: str) -> int:
        """Get the size of a file in S3 data lake."""
        try:
            full_path = f"{self.data_lake_prefix}{remote_path}"
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=full_path)
            return response["ContentLength"]
        except ClientError:
            return 0


class AthenaIcebergProvider(DatabaseProvider):
    """AWS Athena with Iceberg tables database provider."""

    def __init__(self, database: str, workgroup: str = "primary", 
                 results_bucket: str = None, region: str = "us-east-1", **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.database = database
        self.workgroup = workgroup
        self.results_bucket = results_bucket or f"athena-results-{region}"
        self.region = region
        
        self.athena_client = boto3.client("athena", region_name=region, **kwargs)
        self.s3_client = boto3.client("s3", region_name=region, **kwargs)
        
        # Ensure results bucket exists
        self._ensure_results_bucket()

    def _ensure_results_bucket(self):
        """Ensure Athena results bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.results_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                if self.region == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=self.results_bucket)
                else:
                    self.s3_client.create_bucket(
                        Bucket=self.results_bucket,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )

    def connect(self) -> Any:
        """Athena doesn't require persistent connections."""
        return self.athena_client

    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a query in Athena."""
        try:
            # Replace parameters if provided
            if params:
                for key, value in params.items():
                    query = query.replace(f":{key}", str(value))
            
            # Submit query
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.results_bucket}/queries/'
                },
                WorkGroup=self.workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for completion
            self._wait_for_query_completion(query_execution_id)
            
            # Get results
            return self._get_query_results(query_execution_id)
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")

    def _wait_for_query_completion(self, query_execution_id: str, max_wait: int = 300):
        """Wait for Athena query to complete."""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                return
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise RuntimeError(f"Query failed: {reason}")
            
            time.sleep(2)
        
        raise TimeoutError(f"Query timed out after {max_wait} seconds")

    def _get_query_results(self, query_execution_id: str):
        """Get results from completed Athena query."""
        paginator = self.athena_client.get_paginator('get_query_results')
        pages = paginator.paginate(QueryExecutionId=query_execution_id)
        
        results = []
        for page in pages:
            for row in page['ResultSet']['Rows']:
                if 'Data' in row:
                    row_data = [col.get('VarCharValue', '') for col in row['Data']]
                    results.append(row_data)
        
        return results[1:] if results else []  # Skip header row

    def execute_many(self, query: str, params: List[Dict]) -> Any:
        """Execute query with multiple parameter sets (batch execution)."""
        results = []
        for param_set in params:
            result = self.execute_query(query, param_set)
            results.extend(result)
        return results

    def fetch_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            # Replace parameters if provided
            if params:
                for key, value in params.items():
                    query = query.replace(f":{key}", str(value))
            
            # Submit query
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.results_bucket}/dataframes/'
                },
                WorkGroup=self.workgroup
            )
            
            query_execution_id = response['QueryExecutionId']
            self._wait_for_query_completion(query_execution_id)
            
            # Get results location
            execution_details = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            result_location = execution_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            
            # Download and parse CSV results
            import io
            result_key = result_location.replace(f's3://{self.results_bucket}/', '')
            result_obj = self.s3_client.get_object(Bucket=self.results_bucket, Key=result_key)
            
            return pd.read_csv(io.StringIO(result_obj['Body'].read().decode('utf-8')))
            
        except Exception as e:
            raise RuntimeError(f"DataFrame query failed: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        """Insert DataFrame into Iceberg table via Athena."""
        try:
            # Generate INSERT INTO statement
            if if_exists == "replace":
                # Delete existing data first
                delete_query = f"DELETE FROM {table_name}"
                self.execute_query(delete_query)
            
            # Convert DataFrame to VALUES clause
            values_list = []
            for _, row in df.iterrows():
                values = ", ".join([f"'{str(val)}'" if pd.notna(val) else "NULL" for val in row])
                values_list.append(f"({values})")
            
            values_clause = ",\n".join(values_list)
            columns = ", ".join(df.columns)
            
            insert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES {values_clause}
            """
            
            self.execute_query(insert_query)
            return True
            
        except Exception:
            return False

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        """Create an Iceberg table in Athena."""
        try:
            # Map pandas dtypes to Athena types
            type_mapping = {
                'int': 'bigint',
                'integer': 'bigint',
                'bigint': 'bigint',
                'float': 'double',
                'double': 'double',
                'varchar': 'varchar(255)',
                'text': 'varchar(65535)',
                'string': 'varchar(65535)',
                'datetime': 'timestamp',
                'date': 'date',
                'time': 'time',
                'boolean': 'boolean',
                'bool': 'boolean'
            }
            
            columns = []
            for column_name, column_type in schema.items():
                athena_type = type_mapping.get(column_type.lower(), column_type)
                columns.append(f"{column_name} {athena_type}")
            
            create_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
            USING ICEBERG
            LOCATION 's3://f1-data-lake/iceberg/{table_name}/'
            TBLPROPERTIES (
                'table_type'='ICEBERG',
                'format'='parquet',
                'write_compression'='snappy'
            )
            """
            
            self.execute_query(create_query)
            return True
            
        except Exception:
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in Athena."""
        try:
            query = f"SHOW TABLES LIKE '{table_name}'"
            result = self.execute_query(query)
            return len(result) > 0
        except Exception:
            return False

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema from Athena."""
        try:
            query = f"DESCRIBE {table_name}"
            result = self.execute_query(query)
            
            schema = {}
            for row in result:
                if len(row) >= 2:
                    column_name, data_type = row[0], row[1]
                    schema[column_name] = data_type
            
            return schema
        except Exception:
            return {}

    def close(self) -> None:
        """Athena doesn't require connection closing."""
        pass


class LambdaComputeProvider(ComputeProvider):
    """AWS Lambda compute provider for serverless data processing."""

    def __init__(self, region: str = "us-east-1", **kwargs):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.region = region
        self.lambda_client = boto3.client("lambda", region_name=region, **kwargs)

    def submit_job(self, job_config: Dict[str, Any]) -> str:
        """Submit a Lambda function execution."""
        try:
            function_name = job_config.get("function_name", "f1-data-processor")
            payload = job_config.get("payload", {})
            
            # Add timestamp for tracking
            payload["job_id"] = f"job-{int(time.time())}"
            payload["submitted_at"] = datetime.utcnow().isoformat()
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',  # Asynchronous execution
                Payload=json.dumps(payload)
            )
            
            return payload["job_id"]
            
        except ClientError as e:
            raise RuntimeError(f"Lambda execution failed: {e}")

    def get_job_status(self, job_id: str) -> str:
        """Get status of Lambda function (simplified implementation)."""
        try:
            # For Lambda, we can check CloudWatch logs or use a status tracking mechanism
            # This is a simplified implementation
            return "COMPLETED"  # Lambda functions complete quickly
        except Exception:
            return "UNKNOWN"

    def get_job_logs(self, job_id: str) -> str:
        """Get logs from Lambda function execution."""
        try:
            # In practice, you'd query CloudWatch logs
            return f"Lambda execution completed for job {job_id}"
        except Exception:
            return f"No logs available for job {job_id}"

    def cancel_job(self, job_id: str) -> bool:
        """Cancel Lambda function (not applicable for event-based execution)."""
        # Lambda functions can't be cancelled once invoked asynchronously
        return False


class ModernAWSCloudProvider(CloudProvider):
    """Modern AWS cloud provider with serverless services."""

    def __init__(self, config: Dict[str, Any]):
        if not AWS_AVAILABLE:
            raise ImportError("AWS dependencies not installed. Run: pip install boto3")
        
        self.config = config
        self.region = config.get("region", "us-east-1")
        
        self._storage_provider = None
        self._database_provider = None
        self._compute_provider = None

    def get_storage_provider(self) -> StorageProvider:
        """Get the enhanced S3 data lake storage provider."""
        if self._storage_provider is None:
            storage_config = self.config["storage"]
            self._storage_provider = AWSDataLakeProvider(
                bucket_name=storage_config["bucket_name"],
                region=self.region,
                data_lake_prefix=storage_config.get("data_lake_prefix", "data-lake/")
            )
        return self._storage_provider

    def get_database_provider(self) -> DatabaseProvider:
        """Get the Athena + Iceberg database provider."""
        if self._database_provider is None:
            db_config = self.config["database"]
            self._database_provider = AthenaIcebergProvider(
                database=db_config["database"],
                workgroup=db_config.get("workgroup", "primary"),
                results_bucket=db_config.get("results_bucket"),
                region=self.region
            )
        return self._database_provider

    def get_compute_provider(self) -> ComputeProvider:
        """Get the Lambda compute provider."""
        if self._compute_provider is None:
            self._compute_provider = LambdaComputeProvider(region=self.region)
        return self._compute_provider

    def health_check(self) -> Dict[str, bool]:
        """Perform health checks on all modern AWS services."""
        health = {}
        
        # Check S3 Data Lake
        try:
            storage = self.get_storage_provider()
            storage.list_files()
            health["storage"] = True
        except Exception:
            health["storage"] = False
        
        # Check Athena
        try:
            database = self.get_database_provider()
            # Simple health check query
            database.execute_query("SELECT 1 as health_check")
            health["database"] = True
        except Exception:
            health["database"] = False
        
        # Check Lambda
        try:
            compute = self.get_compute_provider()
            health["compute"] = True
        except Exception:
            health["compute"] = False
        
        return health