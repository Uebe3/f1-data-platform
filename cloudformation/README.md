# F1 Data Platform - Modern AWS Infrastructure

This directory contains CloudFormation templates to deploy the modern AWS architecture with serverless services.

## Architecture Components

### 1. Data Lake Foundation (`01-data-lake-foundation.yaml`)
- S3 buckets with proper lifecycle policies
- Glue Data Catalog database
- IAM roles and policies
- Athena workgroup configuration

### 2. Lambda Functions (`02-lambda-functions.yaml`)
- F1 data extraction Lambda
- Data transformation Lambda
- Event-driven processing functions

### 3. Glue ETL Jobs (`03-glue-etl.yaml`)
- Glue jobs for data transformation
- Glue crawlers for schema discovery
- Job scheduling with EventBridge

### 4. Analytics Infrastructure (`04-analytics.yaml`)
- Athena Iceberg tables
- QuickSight dashboards
- Cost optimization settings

## Deployment Order

1. **Foundation First**:
   ```bash
   aws cloudformation create-stack \
     --stack-name f1-data-lake-foundation \
     --template-body file://01-data-lake-foundation.yaml \
     --capabilities CAPABILITY_IAM
   ```

2. **Lambda Functions**:
   ```bash
   aws cloudformation create-stack \
     --stack-name f1-lambda-functions \
     --template-body file://02-lambda-functions.yaml \
     --capabilities CAPABILITY_IAM
   ```

3. **ETL Pipeline**:
   ```bash
   aws cloudformation create-stack \
     --stack-name f1-glue-etl \
     --template-body file://03-glue-etl.yaml \
     --capabilities CAPABILITY_IAM
   ```

4. **Analytics Layer**:
   ```bash
   aws cloudformation create-stack \
     --stack-name f1-analytics \
     --template-body file://04-analytics.yaml \
     --capabilities CAPABILITY_IAM
   ```

## Cost Optimization

The templates include:
- ✅ S3 Intelligent Tiering
- ✅ Lambda provisioned concurrency only where needed
- ✅ Glue job auto-scaling
- ✅ Athena result caching
- ✅ CloudWatch log retention policies

## Security

- ✅ Least privilege IAM roles
- ✅ Encrypted S3 buckets
- ✅ VPC endpoints for private communication
- ✅ Resource tagging for cost allocation