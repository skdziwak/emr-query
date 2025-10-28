"""EMR Serverless Spark Query Runner.

This module executes Spark SQL queries on partitioned parquet data stored in S3.
It loads execution configuration, processes datasets with partition information,
executes SQL views and queries, and writes results back to S3.

Configuration is loaded from S3 or local filesystem in JSON format.
All logging output goes to stderr for EMR compatibility.
"""

import json
import sys
from itertools import product
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_APP_NAME = "EMR Serverless Query Runner"
DEFAULT_SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
}
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def load_config_from_s3(spark: SparkSession, s3_path: str) -> Dict[str, Any]:
    """Load execution configuration from S3 using Spark.

    Args:
        spark: Active Spark session
        s3_path: S3 URI to configuration JSON file (s3://bucket/path/to/config.json)

    Returns:
        Parsed configuration dictionary

    Raises:
        json.JSONDecodeError: If configuration content is not valid JSON
        Exception: If S3 file cannot be read by Spark
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_path}")

    sc = spark.sparkContext
    rdd = sc.textFile(s3_path)
    content = "\n".join(rdd.collect())

    if not content.strip():
        raise ValueError(f"Configuration file is empty: {s3_path}")

    return json.loads(content)


def load_config_local(config_path: str) -> Dict[str, Any]:
    """Load execution configuration from local filesystem.

    Args:
        config_path: Local file path to configuration JSON file

    Returns:
        Parsed configuration dictionary

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        json.JSONDecodeError: If file content is not valid JSON
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        if not content.strip():
            raise ValueError(f"Configuration file is empty: {config_path}")

        return json.loads(content)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")


def build_partition_paths(
    base_path: str, partitions: List[Dict[str, Any]]
) -> List[Tuple[str, Dict[str, Any]]]:
    """Generate all partition path combinations from partition definitions.

    Partitions are specified with a key and list of values. This function generates
    the Cartesian product of all partition values and constructs S3 paths following
    the Hive partitioning format: key1=value1/key2=value2/...

    Args:
        base_path: Base S3 path for the dataset
        partitions: List of partition definitions with 'key' and 'values' fields
                   Example: [{'key': 'year', 'values': [2023, 2024]},
                            {'key': 'month', 'values': [1, 2]}]

    Returns:
        List of tuples containing (full_path, partition_dict) for each combination
        Returns [(base_path, {})] if no partitions specified

    Raises:
        KeyError: If partition definition missing required 'key' or 'values' fields
    """
    if not base_path:
        raise ValueError("base_path cannot be empty")

    if not partitions:
        return [(base_path.rstrip("/"), {})]

    for i, partition in enumerate(partitions):
        if "key" not in partition:
            raise KeyError(f"Partition at index {i} missing required field 'key'")
        if "values" not in partition:
            raise KeyError(f"Partition at index {i} missing required field 'values'")
        if not partition["values"]:
            raise ValueError(f"Partition '{partition['key']}' has empty values list")

    partition_keys = [p["key"] for p in partitions]
    partition_values_lists = [p["values"] for p in partitions]

    paths_and_partitions = []
    for value_combo in product(*partition_values_lists):
        partition_parts = [
            f"{key}={value}" for key, value in zip(partition_keys, value_combo)
        ]
        full_path = f"{base_path.rstrip('/')}/{'/'.join(partition_parts)}"
        partition_dict = dict(zip(partition_keys, value_combo))
        paths_and_partitions.append((full_path, partition_dict))

    return paths_and_partitions


def load_dataset(spark: SparkSession, dataset_config: Dict[str, Any]) -> DataFrame:
    """Load a dataset from S3 with optional partition handling.

    Loads parquet data from all partition combinations, adds partition columns
    to each DataFrame, and unions them together. Partition loading failures are
    logged but don't stop the process - at least one partition must load successfully.

    Args:
        spark: Active Spark session
        dataset_config: Dataset configuration dict with fields:
                       - name: Dataset identifier
                       - path: Base S3 path
                       - partitions (optional): List of partition definitions

    Returns:
        Unified DataFrame containing all partition data with partition columns added

    Raises:
        KeyError: If required config fields 'name' or 'path' are missing
        ValueError: If no partitions load successfully
    """
    if "name" not in dataset_config:
        raise KeyError("Dataset config missing required field 'name'")
    if "path" not in dataset_config:
        raise KeyError("Dataset config missing required field 'path'")

    name = dataset_config["name"]
    base_path = dataset_config["path"]
    partitions = dataset_config.get("partitions", [])

    print(f"Loading dataset: {name}", file=sys.stderr)
    print(f"Base path: {base_path}", file=sys.stderr)

    paths_and_partitions = build_partition_paths(base_path, partitions)
    print(f"Loading {len(paths_and_partitions)} partition(s)", file=sys.stderr)

    dfs = []
    failed_count = 0

    for path, partition_dict in paths_and_partitions:
        print(f"  Loading: {path}", file=sys.stderr)
        try:
            df = spark.read.parquet(path)

            for key, value in partition_dict.items():
                df = df.withColumn(key, F.lit(value))

            dfs.append(df)
        except Exception as e:
            failed_count += 1
            print(f"  Warning: Failed to load {path}: {e}", file=sys.stderr)
            continue

    if not dfs:
        raise ValueError(
            f"No data loaded for dataset '{name}'. "
            f"All {len(paths_and_partitions)} partition(s) failed to load."
        )

    if failed_count > 0:
        print(
            f"  Note: {failed_count}/{len(paths_and_partitions)} partition(s) failed to load",
            file=sys.stderr,
        )

    result_df = dfs[0]
    for df in dfs[1:]:
        result_df = result_df.unionByName(df, allowMissingColumns=True)

    print(f"Dataset '{name}' loaded successfully", file=sys.stderr)

    return result_df


def create_spark_session(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """Create and configure Spark session for EMR Serverless.

    Configures Spark with adaptive query execution, S3 access via hadoop-aws,
    and AWS credential chain authentication.

    Args:
        app_name: Application name for the Spark session

    Returns:
        Configured SparkSession instance
    """
    builder = SparkSession.builder.appName(app_name)

    for key, value in DEFAULT_SPARK_CONFIGS.items():
        builder = builder.config(key, value)

    builder = builder.config("spark.jars.packages", SPARK_PACKAGES)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def execute_query(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    """Execute SQL query workflow: load datasets, execute views, run main query.

    Workflow:
    1. Load all datasets from configuration and register as temp views
    2. Execute SQL views in order (if present in config)
    3. Execute main query and return results

    Args:
        spark: Active Spark session
        config: Execution configuration with fields:
               - datasets: List of dataset configurations
               - views (optional): List of view definitions with 'name' and 'sql'
               - query: Main SQL query to execute

    Returns:
        DataFrame containing query results

    Raises:
        KeyError: If required config fields are missing
        Exception: If dataset loading, view execution, or query execution fails
    """
    if "datasets" not in config:
        raise KeyError("Configuration missing required field 'datasets'")
    if "query" not in config:
        raise KeyError("Configuration missing required field 'query'")

    for dataset_config in config["datasets"]:
        df = load_dataset(spark, dataset_config)
        view_name = dataset_config["name"]
        df.createOrReplaceTempView(view_name)
        print(f"Registered temp view: {view_name}", file=sys.stderr)

    views = config.get("views", [])
    if views:
        print("\nExecuting SQL views:", file=sys.stderr)
        for view in views:
            if "name" not in view:
                raise KeyError("View definition missing required field 'name'")
            if "sql" not in view:
                raise KeyError("View definition missing required field 'sql'")

            view_name = view["name"]
            view_sql = view["sql"]

            if not view_sql.strip():
                print(f"  Skipping empty view: {view_name}", file=sys.stderr)
                continue

            print(f"  Executing view: {view_name}", file=sys.stderr)
            try:
                spark.sql(view_sql)
                print("  ✓ View executed successfully", file=sys.stderr)
            except Exception as e:
                print(f"  ✗ Failed to execute view: {e}", file=sys.stderr)
                raise RuntimeError(
                    f"View execution failed for '{view_name}': {e}"
                ) from e

    query = config["query"]
    if not query.strip():
        raise ValueError("Query cannot be empty")

    print(f"\nExecuting query:\n{query}\n", file=sys.stderr)

    try:
        result_df = spark.sql(query)
        return result_df
    except Exception as e:
        print(f"Query execution failed: {e}", file=sys.stderr)
        raise


def write_results(df: DataFrame, output_path: str) -> None:
    """Write query results to S3 as Parquet format.

    Args:
        df: DataFrame to write
        output_path: S3 path for output (will be overwritten if exists)

    Raises:
        Exception: If write operation fails
    """
    if not output_path:
        raise ValueError("output_path cannot be empty")

    print(f"Writing results to: {output_path}", file=sys.stderr)

    try:
        df.write.mode("overwrite").parquet(output_path)
        print("Results written successfully", file=sys.stderr)
    except Exception as e:
        print(f"Failed to write results: {e}", file=sys.stderr)
        raise


def validate_config(config: Dict[str, Any]) -> None:
    """Validate execution configuration has all required fields.

    Args:
        config: Configuration dictionary to validate

    Raises:
        KeyError: If required fields are missing
        ValueError: If field values are invalid
    """
    required_fields = ["datasets", "query", "outputPrefix"]
    for field in required_fields:
        if field not in config:
            raise KeyError(f"Configuration missing required field '{field}'")

    if not isinstance(config["datasets"], list):
        raise ValueError("'datasets' must be a list")

    if not config["datasets"]:
        raise ValueError("'datasets' cannot be empty")

    if not isinstance(config["query"], str):
        raise ValueError("'query' must be a string")

    if not config["query"].strip():
        raise ValueError("'query' cannot be empty")

    if not isinstance(config["outputPrefix"], str):
        raise ValueError("'outputPrefix' must be a string")

    if not config["outputPrefix"].strip():
        raise ValueError("'outputPrefix' cannot be empty")


def main() -> None:
    """Main entry point for EMR Serverless query runner.

    Expected arguments:
        config_path: S3 URI or local path to execution config JSON
        output_uuid: Unique identifier for this execution run

    Exit codes:
        0: Success
        1: Execution error
    """
    if len(sys.argv) < 3:
        print("Usage: runner.py <config_path> <output_uuid>", file=sys.stderr)
        sys.exit(1)

    config_path = sys.argv[1]
    output_uuid = sys.argv[2]

    if not config_path:
        print("Error: config_path cannot be empty", file=sys.stderr)
        sys.exit(1)

    if not output_uuid:
        print("Error: output_uuid cannot be empty", file=sys.stderr)
        sys.exit(1)

    print(f"Loading configuration from: {config_path}", file=sys.stderr)
    print(f"Output UUID: {output_uuid}", file=sys.stderr)

    spark = None
    try:
        print("\nInitializing Spark session...", file=sys.stderr)
        spark = create_spark_session()

        if config_path.startswith("s3://"):
            print("Loading execution config from S3...", file=sys.stderr)
            config = load_config_from_s3(spark, config_path)
        else:
            print("Loading execution config from local file...", file=sys.stderr)
            config = load_config_local(config_path)

        validate_config(config)

        result_df = execute_query(spark, config)

        output_prefix = config["outputPrefix"].rstrip("/")
        output_path = f"{output_prefix}/{output_uuid}"
        write_results(result_df, output_path)

        print("\nJob completed successfully!", file=sys.stderr)
        print(f"Output location: {output_path}", file=sys.stderr)

        print(output_path, file=sys.stdout)

    except Exception as e:
        print(f"\nError during execution: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
