## 🔧 Configuration

### Environment Variables
```bash
# Basic configuration
F1_PIPELINE_ENV=local          # local, development, production
F1_PIPELINE_LOG_LEVEL=INFO     # DEBUG, INFO, WARNING, ERROR

# Cloud provider selection
F1_CLOUD_PROVIDER=local        # local, aws, azure, gcp

# AWS Configuration (if using AWS)
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-f1-bucket
AWS_RDS_ENDPOINT=your-rds-endpoint

# Azure Configuration (if using Azure)
AZURE_STORAGE_ACCOUNT=yourstorageaccount
AZURE_STORAGE_CONTAINER=f1-data
AZURE_SQL_SERVER=your-sql-server

# GCP Configuration (if using GCP)
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-f1-bucket
GCP_DATASET_ID=f1_dataset
```

### Configuration File
Create `config.yaml` for advanced configuration:

```yaml
environment: local
log_level: INFO

storage:
  provider: local
  local_path: ./data
  # aws:
  #   bucket_name: f1-data-bucket
  #   region: us-east-1

database:
  provider: local
  db_path: ./f1_data.db
  # aws_rds:
  #   endpoint: your-endpoint.rds.amazonaws.com
  #   database: f1_analytics
  #   username: f1_user

api:
  base_url: https://api.openf1.org/v1
  rate_limit: 100
  retry_attempts: 3
  timeout: 30

processing:
  batch_size: 1000
  parallel_workers: 4
  memory_limit_mb: 2048
```

## 🧪 Testing

### Run All Tests
```bash
python run_tests.py
```

### Run Specific Test Types
```bash
# Unit tests only
python run_tests.py --type unit

# Integration tests only  
python run_tests.py --type integration

# Fast tests (excluding slow integration tests)
python run_tests.py --type fast

# Cloud-specific tests
python run_tests.py --type aws
python run_tests.py --type azure
python run_tests.py --type local
```

### Test Coverage
```bash
# Run tests with coverage report
python run_tests.py --verbose

# View HTML coverage report
open htmlcov/index.html  # macOS/Linux
start htmlcov/index.html # Windows
```

## 📈 Usage Examples

### Basic Data Extraction
```python
from f1_pipeline.config.settings import Settings
from f1_pipeline.cloud_swap import CloudProviderFactory
from f1_pipeline.extractors import DataExtractor

# Setup
settings = Settings(environment="local")
cloud_provider = CloudProviderFactory.create("local", settings.get_cloud_provider_config())
extractor = DataExtractor(settings, cloud_provider)

# Extract data for 2023 season
stats = extractor.extract_year_data(2023, save_raw=True, save_to_db=True)
print(f"Processed {stats['endpoints_processed']} endpoints")
print(f"Total records: {stats['total_records']}")
```

### Analytics Layer Processing
```python
from f1_pipeline.transformers import DataTransformer

# Initialize transformer
transformer = DataTransformer(settings, cloud_provider)

# Create analytics tables
transformer.setup_analytics_tables()

# Transform specific session data
session_key = 9158  # Example session
analytics_data = transformer.process_session_analytics(session_key)
```

### AI Feature Engineering
```python
from f1_pipeline.transformers import AIPreparationTransformer

# Initialize AI transformer
ai_transformer = AIPreparationTransformer(settings, cloud_provider)

# Setup AI feature tables
ai_transformer.setup_ai_tables()

# Generate driver performance features
driver_features = ai_transformer.create_driver_performance_features(2023)
print(f"Generated features for {len(driver_features)} drivers")
```

### Cloud Provider Switching
```python
# Switch between cloud providers without code changes
aws_settings = Settings(
    environment="production",
    storage=StorageConfig(
        provider="aws",
        bucket_name="production-f1-data",
        region="us-east-1"
    ),
    database=DatabaseConfig(
        provider="aws_rds",
        endpoint="prod.rds.amazonaws.com",
        database="f1_analytics"
    )
)

aws_provider = CloudProviderFactory.create("aws", aws_settings.get_cloud_provider_config())
```

### Health Monitoring
```python
# Check system health
health_status = extractor.health_check()
print(f"API Status: {health_status['api_accessible']}")
print(f"Storage Status: {health_status['storage_accessible']}")

# Check cloud provider health
cloud_health = cloud_provider.health_check()
print(f"Storage Health: {cloud_health['storage']}")
print(f"Database Health: {cloud_health['database']}")
```

## 📁 Project Structure

```
f1_pipeline/
├── __init__.py                 # Package initialization
├── config/
│   ├── __init__.py
│   └── settings.py            # Configuration management
├── cloud_swap/                # Cloud abstraction layer
│   ├── __init__.py
│   ├── interfaces/            # Abstract base classes
│   ├── providers/             # Cloud-specific implementations
│   └── factory.py            # Provider factory
├── extractors/                # Data extraction
│   ├── __init__.py
│   ├── openf1_client.py      # OpenF1 API client
│   └── data_extractor.py     # Main extraction logic
├── models/                    # Data models and schemas
│   ├── __init__.py
│   ├── raw_data.py           # Raw data models
│   ├── analytics.py          # Analytics models
│   ├── ai_features.py        # AI feature models
│   └── schemas.py            # Schema management
├── transformers/              # Data transformation
│   ├── __init__.py
│   ├── data_transformer.py   # Analytics transformation
│   └── ai_transformer.py     # AI feature engineering
├── storage/                   # Storage utilities
│   ├── __init__.py
│   └── utils.py              # Storage helper functions
└── utils/                     # General utilities
    ├── __init__.py
    ├── logging.py            # Logging configuration
    └── helpers.py            # Helper functions

tests/
├── __init__.py
├── conftest.py               # Test configuration
├── pytest.ini               # Pytest configuration  
├── unit/                     # Unit tests
│   ├── test_openf1_client.py
│   └── test_cloud_swap.py
└── integration/              # Integration tests
    └── test_pipeline_integration.py

docs/                         # Documentation
├── architecture.md          # Architecture details
├── api_reference.md         # API documentation
└── deployment.md           # Deployment guides
```

## 🔄 Data Flow

1. **Extraction**: OpenF1 API → Raw Data Storage
2. **Analytics Processing**: Raw Data → Analytics Tables
3. **Feature Engineering**: Analytics Data → AI Features
4. **AI/ML Consumption**: Features → Models → Insights

Each step is independently executable and resumable, enabling flexible processing workflows.

## 🛠️ Development

### Adding New Cloud Providers
1. Implement `StorageProvider` and `DatabaseProvider` interfaces
2. Create provider-specific implementation in `cloud_swap/providers/`
3. Register in `CloudProviderFactory`
4. Add configuration schema in `settings.py`

### Adding New Data Sources
1. Create extractor class implementing data source interface
2. Add data models in `models/` 
3. Implement transformation logic in `transformers/`
4. Add comprehensive tests

### Custom Analytics
1. Extend `DataTransformer` with custom analytics methods
2. Add new analytics models in `models/analytics.py`
3. Create database tables via `SchemaManager`
4. Implement validation and testing

## 📋 API Reference

### Core Classes

#### `DataExtractor`
Main class for extracting F1 data from OpenF1 API.

```python
class DataExtractor:
    def extract_year_data(self, year: int, save_raw: bool = True, save_to_db: bool = True) -> Dict
    def extract_session_data(self, session_key: int) -> Dict
    def health_check(self) -> Dict
```

#### `DataTransformer`
Processes raw data into analytics-ready format.

```python
class DataTransformer:
    def setup_analytics_tables(self) -> None
    def process_session_analytics(self, session_key: int) -> pd.DataFrame
    def calculate_lap_times(self, session_key: int) -> pd.DataFrame
```

#### `AIPreparationTransformer` 
Generates AI/ML ready features.

```python
class AIPreparationTransformer:
    def setup_ai_tables(self) -> None
    def create_driver_performance_features(self, year: int) -> pd.DataFrame
    def create_team_strategy_features(self, year: int) -> pd.DataFrame
```

## 🚀 Deployment

### Local Development
```bash
# Set environment
export F1_PIPELINE_ENV=local
export F1_CLOUD_PROVIDER=local

# Run extraction
python -m f1_pipeline.extractors.data_extractor --year 2023
```

### AWS Deployment
```bash
# Configure AWS credentials
aws configure

# Set environment variables
export F1_PIPELINE_ENV=production
export F1_CLOUD_PROVIDER=aws
export AWS_S3_BUCKET=production-f1-data
export AWS_RDS_ENDPOINT=prod.rds.amazonaws.com

# Deploy
python deploy_aws.py
```

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY f1_pipeline/ ./f1_pipeline/
COPY config.yaml .

CMD ["python", "-m", "f1_pipeline.extractors.data_extractor", "--year", "2023"]
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

### Development Setup
```bash
# Clone and setup
git clone <repository-url>
cd showcase-f1-pipeline
pip install -r requirements-dev.txt

# Run tests
python run_tests.py --type unit

# Run linting
flake8 f1_pipeline/
black f1_pipeline/
mypy f1_pipeline/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏁 Acknowledgments

- **OpenF1 API**: Providing comprehensive F1 data
- **Formula 1**: For the amazing sport that makes this data possible
- **Contributors**: Everyone who helps improve this project

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/username/f1-pipeline/issues)
- **Documentation**: [Full Documentation](docs/)
- **API Reference**: [API Docs](docs/api_reference.md)

---

Built with ❤️ for Formula 1 fans and data enthusiasts!