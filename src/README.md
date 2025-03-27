# E2E Platform - ETL Module

This ETL (Extract, Transform, Load) module is a critical component of the e2e_platform project, designed to process and analyze large-scale data workflows using Dagster, an orchestration framework. The system handles data from extraction through transformation to final loading into both S3 and PostgreSQL.

## Detailed Architecture

The module follows a modular, microservices-oriented architecture:

```
etl/
├── assets/                    # Dagster assets (core data pipeline components)
│   ├── core/                 # Core business logic assets
│   │   ├── __init__.py
│   │   ├── extract.py       # Data extraction logic
│   │   ├── transform.py     # Data transformation operations
│   │   ├── upload.py        # S3 upload functionality
│   │   └── load_postgres.py # Database loading operations
│   └── __init__.py
├── resources/                # Configurable resource definitions
│   ├── __init__.py
│   ├── ts_resources.py      # Time series download resources
│   ├── transform_resource.py # Transformation resources
│   ├── upload_resource.py   # S3 upload resources
│   └── load_resource.py     # PostgreSQL loading resources
├── setting/                  # Configuration and settings
│   ├── __init__.py
│   └── setting.py           # Global configuration
├── test/                    # Test suite
│   ├── __init__.py
│   ├── conftest.py         # Test configuration
│   └── test_*.py          # Test modules
└── utils/                  # Utility functions
    ├── __init__.py
    └── helpers.py         # Common helper functions
```

## Core Components in Detail

### Assets
- **extract.py**: 
  - Handles data extraction from multiple sources
  - Supports parallel downloads
  - Implements retry logic
  - Validates downloaded data

- **transform.py**: 
  - Performs data cleaning and normalization
  - Handles missing data
  - Implements data type conversions
  - Supports parallel processing

- **upload.py**: 
  - Manages S3 uploads with chunking
  - Implements retry mechanism
  - Handles multipart uploads
  - Validates uploaded files

- **load_postgres.py**: 
  - Manages database connections
  - Implements bulk loading
  - Handles transaction management
  - Performs data validation

### Resources
- **ParquetDownloadResource**: 
  ```python
  ParquetDownloadResource(
      base_url: str,          # Base URL for downloads
      years: List[int],       # Years to process
      months: List[int],      # Months to process
      download_folder: str    # Local storage path
  )
  ```

- **ParquetTransformResource**: 
  ```python
  ParquetTransformResource(
      source_folder: str,     # Input folder path
      output_folder: str,     # Output folder path
      max_workers: int        # Parallel processing workers
  )
  ```

- **ParquetUploadResource**: 
  ```python
  ParquetUploadResource(
      aws_access_key: str,    # AWS credentials
      aws_secret_key: str,    # AWS secret
      region_name: str,       # AWS region
      source_folder: str,     # Local folder path
      bucket_name: str,       # S3 bucket name
      s3_folder: str,         # S3 folder path
      batch_size: int         # Upload batch size
  )
  ```

- **ParquetPostgresLoader**: 
  ```python
  ParquetPostgresLoader(
      host: str,             # Database host
      port: str,             # Database port
      dbname: str,           # Database name
      user: str,             # Database user
      password: str,         # Database password
      output_folder: str     # Data folder path
  )
  ```

## Detailed Prerequisites

### System Requirements
- Python 3.12 or higher
- 8GB RAM minimum (16GB recommended)
- 4 CPU cores minimum
- 20GB free disk space

### Software Dependencies
- Docker 24.0+
- PostgreSQL 15+
- AWS CLI v2
- Git 2.0+

### AWS Configuration
1. IAM Permissions needed:
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": [
                   "s3:PutObject",
                   "s3:GetObject",
                   "s3:ListBucket"
               ],
               "Resource": [
                   "arn:aws:s3:::your-bucket-name/*",
                   "arn:aws:s3:::your-bucket-name"
               ]
           }
       ]
   }
   ```

## Detailed Installation

1. Clone with submodules:
```bash
git clone --recursive <repository-url>
cd e2e_platform
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your settings
```

## Advanced Configuration

### Environment Variables
```bash
# AWS Configuration
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key
REGION=your_aws_region
S3_BUCKET=your_bucket_name

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Application Settings
MAX_WORKERS=4
BATCH_SIZE=10
LOG_LEVEL=INFO
```

### Performance Tuning
- Adjust `MAX_WORKERS` based on CPU cores
- Configure `BATCH_SIZE` based on memory
- Set `POSTGRES_MAX_CONNECTIONS` appropriately

## Development Workflow

### Local Development
1. Start dependencies:
```bash
docker-compose up -d postgres
```

2. Run Dagster development server:
```bash
./run_dagster.sh
```

3. Access Dagster UI:
```bash
open http://localhost:3000
```

### Testing Strategy

#### Unit Tests
```bash
# Run specific test
pytest etl/test/test_upload.py -v

# Run with coverage
./run_test.sh
```

#### Integration Tests
```bash
pytest etl/test/integration/ -v
```

#### Performance Tests
```bash
pytest etl/test/performance/ -v --durations=0
```

## Monitoring and Logging

### Dagster Monitoring
- Access metrics at `/dagster-debug`
- View asset lineage
- Monitor pipeline performance

### Logging Configuration
```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'etl.log',
        }
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    }
}
```

## Error Handling and Recovery

### Retry Mechanism
- Configurable retry attempts
- Exponential backoff
- Dead letter queues for failed operations

### Data Validation
- Schema validation
- Data quality checks
- Integrity constraints

## Performance Optimization

### Batch Processing
- Configurable batch sizes
- Memory optimization
- Parallel processing

### Database Optimization
- Bulk loading
- Index management
- Connection pooling

## Contributing Guidelines

### Code Style
- Follow PEP 8
- Use type hints
- Document all functions

### Pull Request Process
1. Create feature branch
2. Update documentation
3. Add tests
4. Submit PR with description

## Troubleshooting

### Common Issues
1. Connection timeouts
2. Memory errors
3. Permission issues

### Debug Tools
- Dagster debug interface
- Logging analysis
- Performance profiling

## License

[Your License Information]

## Contact

- **Author**: [Your Name]
- **Email**: [Your Email]
- **GitHub**: [Your GitHub]