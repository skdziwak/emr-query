# emr-query

A lightweight PySpark-powered tool for rapid iteration on complex Spark SQL queries using AWS EMR Serverless.

**⚠️ NOT INTENDED FOR PRODUCTION USE** - This is a development tool designed to speed up the query development cycle when working with large datasets on S3. For production workloads, use proper EMR job orchestration with appropriate error handling, monitoring, and data quality checks.

## Overview

`emr-query` streamlines the process of testing and refining Spark SQL queries against partitioned Parquet data stored in S3. Instead of manually managing EMR job submissions, artifact uploads, and result downloads, this tool provides a single command interface that:

1. Uploads your query configuration and runner script to S3
2. Submits an EMR Serverless job with optimal Spark settings
3. Monitors job execution and displays Spark UI URL when available
4. Automatically downloads results and logs when the job completes
5. Handles partitioned datasets with automatic path generation

## Key Features

- **Simple Configuration**: Define datasets, partitions, SQL views, and queries in a single YAML file
- **Parameter Substitution**: Use `${PARAM_NAME}` placeholders in SQL queries and views (⚠️ simple string substitution - not SQL injection safe)
- **Automatic Partition Handling**: Specify partition keys and values; paths are generated automatically using Hive format
- **Direct Spark Configuration**: Use actual Spark option names for both submit-time and session-time configs with clear separation
- **Real-time Monitoring**: Track job progress and get Spark UI access as soon as job starts running
- **Automatic Result Download**: Results and logs are automatically downloaded to local directories
- **Nix-based Reproducibility**: Fully reproducible development environment with all dependencies

## Installation

### For NixOS Users or Nix Users

Simply run the tool directly from the repository:

```bash
nix run github:skdziwak/emr-query
```

Or clone and run locally:

```bash
git clone git@github.com:skdziwak/emr-query.git
cd emr-query
nix run
```

### For Non-NixOS Users

First, [install Nix](https://nixos.org/download.html):

```bash
sh <(curl -L https://nixos.org/nix/install) --daemon
```

Then either:

**Option 1**: Run directly without installing:
```bash
nix run github:skdziwak/emr-query
```

**Option 2**: Install to your profile:
```bash
nix profile install github:skdziwak/emr-query
emr-query --help
```

**Option 3**: Use in a development shell:
```bash
git clone git@github.com:skdziwak/emr-query.git
cd emr-query
nix develop
python deploy.py --help
```

## Configuration

Create a YAML configuration file (e.g., `query.yaml`):

```yaml
# AWS EMR Serverless Configuration
applicationId: 00fxxxxxxxxxx
awsRole: arn:aws:iam::123456789012:role/EMRServerlessRole
awsRegion: us-east-1
awsProfile: default  # AWS CLI profile to use

# S3 Paths
outputPrefix: s3://my-bucket/query-results/
logsPath: s3://my-bucket/spark-logs/
buildPath: s3://my-bucket/emr-artifacts/

# Local Directories
localOutputDir: ./output
localLogsDir: ./logs

# Datasets with Partitions
datasets:
  - name: events
    path: s3://my-bucket/data/events/
    partitions:
      - key: date
        values:
          - "2024-01-01"
          - "2024-01-02"
      - key: region
        values:
          - us-east
          - us-west

  - name: users
    path: s3://my-bucket/data/users/
    # No partitions - loads from base path

# Optional: SQL Views (executed before main query)
views:
  - path/to/create_metrics_view.sql
  - path/to/create_aggregates_view.sql

# Optional: Parameters for substitution
parameters:
  START_DATE: "2024-01-01"
  END_DATE: "2024-01-31"
  LIMIT: 1000

# Main Query
query: |
  SELECT
    e.user_id,
    u.username,
    COUNT(*) as event_count
  FROM events e
  JOIN users u ON e.user_id = u.id
  WHERE e.date >= '${START_DATE}'
    AND e.date <= '${END_DATE}'
  GROUP BY e.user_id, u.username
  ORDER BY event_count DESC
  LIMIT ${LIMIT}

# Optional: Deploy-time Spark Configuration (passed to EMR Serverless via --conf)
# If provided, completely replaces defaults (no merging)
sparkSubmitConfig:
  spark.executor.memory: "32G"
  spark.executor.cores: "8"
  spark.driver.memory: "16G"
  spark.driver.cores: "4"
  spark.dynamicAllocation.enabled: "true"
  spark.dynamicAllocation.maxExecutors: "50"
  spark.dynamicAllocation.minExecutors: "2"
  spark.dynamicAllocation.initialExecutors: "10"
  spark.sql.shuffle.partitions: "400"
  spark.default.parallelism: "400"

# Optional: Session-time Spark Configuration (applied during SparkSession creation)
# If provided, completely replaces defaults (no merging)
sparkSessionConfig:
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.parquet.datetimeRebaseModeInWrite: "CORRECTED"
```

### Example SQL View File (`create_metrics_view.sql`)

```sql
CREATE TEMPORARY VIEW daily_metrics AS
SELECT
  date,
  region,
  COUNT(*) as total_events,
  COUNT(DISTINCT user_id) as unique_users
FROM events
WHERE date >= '${START_DATE}'
GROUP BY date, region
```

## Usage

### Basic Usage

```bash
nix run -- query.yaml
```

### With Query Override

Override the query defined in the config file:

```bash
nix run -- query.yaml --query "SELECT COUNT(*) FROM events"
```

### Without Monitoring

Submit the job and exit immediately (don't wait for results):

```bash
nix run -- query.yaml --no-monitor
```

### Custom Polling Interval

Check job status every 60 seconds instead of default 15:

```bash
nix run -- query.yaml --poll-interval 60
```

## How It Works

### Architecture

The tool consists of two main components:

1. **`deploy.py`** (Deployment Orchestrator)
   - Runs on your local machine
   - Generates execution config from your YAML file
   - Uploads artifacts to S3
   - Submits EMR Serverless job
   - Monitors execution and downloads results

2. **`runner.py`** (Spark Driver Script)
   - Runs on EMR Serverless
   - Loads configuration from S3
   - Builds partition paths and loads datasets
   - Executes SQL views and main query
   - Writes results to S3 as Parquet

### Execution Flow

```
┌─────────────────┐
│  query.yaml     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  deploy.py                          │
│  - Read config                      │
│  - Substitute parameters            │
│  - Generate execution config (JSON) │
│  - Upload runner.py to S3           │
│  - Upload execution config to S3    │
│  - Submit EMR job                   │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  EMR Serverless                     │
│  ┌───────────────────────────────┐  │
│  │ runner.py                     │  │
│  │ - Load config from S3         │  │
│  │ - Build partition paths       │  │
│  │ - Load datasets               │  │
│  │ - Execute SQL views           │  │
│  │ - Execute main query          │  │
│  │ - Write results to S3         │  │
│  └───────────────────────────────┘  │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  deploy.py (monitoring)             │
│  - Poll job status                  │
│  - Show Spark UI URL                │
│  - Download results on completion   │
│  - Download logs                    │
└─────────────────────────────────────┘
```

### Partition Path Generation

For the example configuration above with the `events` dataset:

```yaml
datasets:
  - name: events
    path: s3://my-bucket/data/events/
    partitions:
      - key: date
        values: ["2024-01-01", "2024-01-02"]
      - key: region
        values: ["us-east", "us-west"]
```

The tool generates these S3 paths:
- `s3://my-bucket/data/events/date=2024-01-01/region=us-east/`
- `s3://my-bucket/data/events/date=2024-01-01/region=us-west/`
- `s3://my-bucket/data/events/date=2024-01-02/region=us-east/`
- `s3://my-bucket/data/events/date=2024-01-02/region=us-west/`

Each partition combination is loaded separately and union-ed together with partition columns added to the DataFrame.

## Parameter Substitution

⚠️ **IMPORTANT SECURITY NOTE**: The parameter substitution system uses simple string replacement. It does **NOT** provide SQL injection protection. Only use with trusted input and never expose this to user input in production systems.

Parameters are substituted using the format `${PARAM_NAME}`:

```yaml
parameters:
  MIN_SCORE: 100
  CATEGORY: "electronics"

query: |
  SELECT * FROM products
  WHERE score >= ${MIN_SCORE}
    AND category = '${CATEGORY}'
```

This becomes:
```sql
SELECT * FROM products
WHERE score >= 100
  AND category = 'electronics'
```

## Spark Configuration

The tool separates Spark configuration into two categories:

### 1. Deploy-time Configuration (`sparkSubmitConfig`)

These options are passed to EMR Serverless via `--conf` flags when submitting the job. They control resource allocation and execution parameters.

**Default values** (applied when `sparkSubmitConfig` is not specified):

```yaml
sparkSubmitConfig:
  spark.executor.memory: "16G"
  spark.executor.cores: "4"
  spark.driver.memory: "8G"
  spark.driver.cores: "2"
  spark.dynamicAllocation.enabled: "true"
  spark.dynamicAllocation.minExecutors: "1"
  spark.dynamicAllocation.maxExecutors: "100"
  spark.dynamicAllocation.initialExecutors: "10"
  spark.sql.shuffle.partitions: "200"
  spark.default.parallelism: "200"
```

### 2. Session-time Configuration (`sparkSessionConfig`)

These options are applied when creating the SparkSession in runner.py. They control Spark behavior during execution.

**Default values** (applied when `sparkSessionConfig` is not specified):

```yaml
sparkSessionConfig:
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
  spark.sql.parquet.datetimeRebaseModeInWrite: "CORRECTED"
  spark.sql.parquet.datetimeRebaseModeInRead: "CORRECTED"
  spark.sql.parquet.int96RebaseModeInRead: "CORRECTED"
  spark.sql.parquet.int96RebaseModeInWrite: "CORRECTED"
```

### Important Notes

- **All configuration keys use direct Spark option names** (e.g., `spark.executor.memory` instead of custom property names)
- **If you provide any configuration in your YAML, defaults are completely replaced** (no merging). This ensures predictable behavior.
- You can specify only `sparkSubmitConfig`, only `sparkSessionConfig`, both, or neither
- Any valid Spark configuration option can be used - no validation is enforced

## Output Structure

### Results Directory

Results are downloaded to `localOutputDir/{uuid}/`:

```
output/
└── a1b2c3d4-e5f6-7890-abcd-ef1234567890/
    ├── _SUCCESS
    ├── part-00000-xxx.snappy.parquet
    ├── part-00001-xxx.snappy.parquet
    └── ...
```

### Logs Directory

Driver logs are downloaded to `localLogsDir/`:

```
logs/
└── 00fxxxxxx_job-run-id_driver_stderr.log
```

## Requirements

- AWS Account with EMR Serverless application configured
- IAM role with appropriate S3 and EMR permissions
- S3 buckets for artifacts, results, and logs
- AWS credentials configured (via `~/.aws/credentials` or environment variables)

## Troubleshooting

### Job Fails Immediately

- Check that your EMR Serverless application ID is correct
- Verify IAM role has permissions for S3 and EMR operations
- Ensure S3 paths in config are accessible

### Partition Loading Errors

- Verify partition paths exist in S3
- Check that parquet files are valid
- Review dataset base path and partition definitions

### Parameter Substitution Issues

- Ensure parameter names in SQL match exactly (case-sensitive)
- Check that all used parameters are defined in config
- Remember: this is string substitution, not prepared statements

### Downloads Fail

- Verify `localOutputDir` and `localLogsDir` paths are writable
- Check network connectivity to S3
- Ensure AWS credentials are valid

## Development

Enter the development shell with all dependencies:

```bash
nix develop
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests on [GitHub](https://github.com/skdziwak/emr-query).

## Acknowledgments

Built with PySpark, boto3, and the Nix ecosystem for reproducible builds.
