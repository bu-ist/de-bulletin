# Changelog

## [v1.1.0] - 2026-02-09
### Major Refactoring - Alignment with de-datalake-bulletin-dataload
- Refactored project structure to match de-datalake-bulletin-dataload while maintaining sensor-based execution
- Replaced `HTTPClientResource` with unified `ConfigResource` that manages both configuration and HTTP client settings
- Added incremental loading logic with S3 auto-discovery and `modified_after` filtering
- Implemented `RuntimeConfig` with `full_refresh` and `last_modified_date` parameters for flexible loading strategies
- Added `utilities.py` with `determine_filter_date()` and `get_last_modified_from_s3()` for intelligent baseline detection
- Updated `config.json` with comprehensive configuration including `idempotency_param`, `default_baseline_datetime`, `partition_date_prefix`, `http_client` settings, etc.
- Created `bulletin_raw_job` definition in `sensors.py` for better job management
- Updated sensor to trigger job instead of asset selection for consistency with schedule-based project
- Renamed asset group from `de_datalake_bulletin` to `bulletin_raw` for consistency
- Renamed assets from `bulletin_pages`/`bulletin_media` to `bulletin_raw_pages`/`bulletin_raw_media`
- Updated `definitions.py` to remove `HTTPClientResource` and add `bulletin_raw_job`
- Enhanced sensor with Kubernetes configuration tags for resource requests/limits
- Added dynamic User-Agent generation from package metadata in `ConfigResource`
- Updated README.md with comprehensive documentation on incremental loading, configuration precedence, and runtime behavior
- Added `ruff>=0.14.14` to dev dependencies for code quality checks
- Version bumped to 1.1.0 to align with de-datalake-bulletin-dataload

### Breaking Changes
- Removed `HTTPClientResource` - use `ConfigResource` directly
- Removed `USER_AGENT` environment variable - now auto-generated from package version
- Changed asset names: `bulletin_pages` → `bulletin_raw_pages`, `bulletin_media` → `bulletin_raw_media`
- Changed asset group: `de_datalake_bulletin` → `bulletin_raw`
- Updated `config.json` schema with new required fields
- Sensor now triggers `bulletin_raw_job` instead of directly selecting assets

### Migration Guide
1. Update `config.json` to include new fields: `idempotency_param`, `default_baseline_datetime`, `partition_date_prefix`, `partition_time_prefix`, `parquet_file_extension`, `http_client` settings
2. Remove `USER_AGENT` from environment variables
3. Update any references to old asset names in deployment configurations
4. Run `uv sync --frozen` to install updated dependencies including ruff

## [v1.2.0] - 2026-02-02
### Performance & Optimization
- Optimized async HTTP fetching with HTTP/2 support (50 max connections, 20 keepalive connections)
- Improved retry logic with exponential backoff (5s-60s, up to 5 attempts) using tenacity
- Simplified S3 upload logic for small parquet files (removed multipart upload)

### Configuration & Architecture
- Migrated `base_url` from environment variable to `config.json` for centralized configuration
- Added validation to `ConfigResource` (base_url, endpoints, pagination fields)
- Added validation to `AWSS3Resource` (lazy client initialization with bucket existence check)
- Fixed `AWSS3Resource` error handling (removed invalid boto3.client type annotation)

### Code Quality
- Standardized all docstrings to Google Style format across codebase
- Removed unused imports (dagster as dg, contextlib, datetime from specific files)
- Updated all function/class docstrings with proper Args, Returns, Raises sections

### Docker & Deployment
- Optimized Dockerfile by removing redundant uv bootstrap in runtime stage
- Fixed permission issues by creating `/opt/dagster` and `/bulletin_raw` directories with proper ownership
- Added `dagster-postgres` dependency for Kubernetes gRPC deployment (instance storage)
- Set `PYTHONPATH=/app/src` for proper module imports

### Bug Fixes
- Fixed sensor to work properly with Kubernetes deployment (uses `context.resources.get_config`)
- Fixed environment variable quote handling in `.env` file
- Updated Kubernetes configmap and deployment manifests for new configuration structure

## [v1.1.5] - 2026-01-28
### Architecture Changes
- Refactored asset logic to create two separate named assets (`bulletin_pages`, `bulletin_media`) instead of dynamic multi-asset configuration
- Simplified asset structure for better maintainability and debugging

### Data Quality
- Added hash function to generate `dl_hash` column for data integrity verification
- Hash computed from concatenation of all column values with hex encoding

### Automation
- Implemented `bulletin_data_sensor` for automated content change detection
- Sensor compares latest modified dates from WordPress API against cursor-stored values
- Only triggers asset materialization when new or modified content is detected
- Uses cursor persistence to track state between sensor evaluations

## [v1.1.4] - 2026-01-27
### Configuration Management
- Introduced `config.json` for endpoint configuration (dynamic endpoint definition)
- Abstracted pagination and sensor parameters into configuration file
- Enabled adding new endpoints without code changes

### Data Standards
- Formatted API responses to conform to DE standard schema: `id`, `dl_inserted_at`, `payload`
- Standardized timestamp field as `dl_inserted_at` for data lineage tracking

### Code Organization
- Created `ConfigResource` for centralized configuration loading
- Removed hard-coded endpoint paths and URL parameters

## [v1.1.3] - 2026-01-23
### Architecture Simplification
- Removed database connections and dependencies (DuckDB removal)
- Eliminated intermediate database storage layer
- Direct pipeline: API → Validation → Parquet → S3

### AWS Integration
- Implemented S3 upload functionality with `AWSS3Resource`
- Created folder structure preservation for data lake organization
- Path format: `/bulletin_raw/{endpoint}/load_date={date}/load_time={time}/`

### Code Structure
- Separated concerns into modular files: `resources.py`, `validators.py`, `data_exporters.py`, `assets.py`
- Improved code maintainability and testability
- Created reusable export functions

## [v1.1.2] - 2026-01-21
### Data Pipeline Changes
- Migrated from DuckDB to PostgreSQL for DE standardization (later removed in v1.1.3)
- Switched Parquet export to Polars library (replacing DuckDB's export functionality)
- Improved Parquet write performance with Polars DataFrame operations

### Documentation
- Updated documentation to align with DE data engineering standards
- Added comprehensive docstrings for functions and classes

## [v1.1.1] - 2026-01-15
### Repository Migration
- Renamed repository from `de-bulletin-ingestion` to `de-datalake-bulletin-dataload`
- Aligned naming convention with DE data engineering standards

### HTTP Client Upgrade
- Replaced `aiohttp` with `httpx` for async HTTP requests
- Improved HTTP/2 support and connection management
- Better exception handling and retry capabilities

### Dagster Integration
- Added Dagster resources (`HTTPClientResource`, `ParquetExportResource`)
- Created `definitions.py` for Dagster Web UI compatibility
- Enabled asset materialization via Dagster interface

## [v1.1.0] - 2026-01-06
### Initial Release
- Created initial pipeline in `de-bulletin-ingestion` repository
- Implemented async data collection from BU Bulletin WordPress API
- Added DuckDB local storage for fetched data
- Created basic documentation and README
- Established project structure for data ingestion workflow

