"""EMR Serverless Deployment Orchestrator.

This module handles the complete lifecycle of deploying and executing Spark jobs
on AWS EMR Serverless:
1. Uploads runner script and execution config to S3
2. Submits EMR Serverless job
3. Monitors job execution status
4. Downloads results and logs

All operational logging goes to stderr, while structured output goes to stdout.
"""

import argparse
import gzip
import json
import os
import re
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
import yaml
from botocore.exceptions import ClientError

DEFAULT_RUNNER_PATH = "runner.py"
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_POLL_INTERVAL = 5
DEFAULT_LOCAL_OUTPUT_DIR = "./output"
DEFAULT_LOCAL_LOGS_DIR = "./logs"

TERMINAL_STATES = frozenset({"SUCCESS", "FAILED", "CANCELLED"})
SUCCESS_STATE = "SUCCESS"

DEFAULT_SPARK_SUBMIT_CONFIG = {
    "executor_memory": "4G",
    "executor_cores": 4,
    "driver_memory": "4G",
    "driver_cores": 2,
    "min_executors": 1,
    "max_executors": 100,
    "initial_executors": 10,
    "shuffle_partitions": 200,
    "parallelism": 200,
}


def log(message: str, file=sys.stderr) -> None:
    """Print operational log message to stderr.

    Args:
        message: Message to log
        file: Output file stream (default: stderr)
    """
    print(message, file=file)


def substitute_parameters(text: str, parameters: Dict[str, Any]) -> str:
    """Substitute ${...} placeholders with parameter values.

    Args:
        text: Text containing ${param_name} placeholders
        parameters: Dictionary mapping parameter names to values

    Returns:
        Text with placeholders replaced by parameter values

    Raises:
        ValueError: If placeholder references undefined parameter
    """
    if not parameters:
        return text

    def replacer(match):
        param_name = match.group(1)
        if param_name not in parameters:
            raise ValueError(
                f"Undefined parameter '${{{param_name}}}' in SQL. "
                f"Available parameters: {list(parameters.keys())}"
            )
        return str(parameters[param_name])

    return re.sub(r"\$\{(\w+)\}", replacer, text)


class EMRServerlessDeployer:
    """Orchestrates deployment and execution of Spark jobs on EMR Serverless.

    This class manages the complete workflow:
    - Artifact preparation and upload
    - Job submission and monitoring
    - Result and log retrieval

    Attributes:
        config: Deployment configuration dictionary
        output_uuid: Unique identifier for this execution
        job_run_id: EMR job run identifier (set after submission)
        application_id: EMR Serverless application ID
        s3_client: Boto3 S3 client
        emr_client: Boto3 EMR Serverless client
    """

    def __init__(
        self,
        config_path: str,
        runner_py_path: Optional[str] = None,
        query_override: Optional[str] = None,
    ) -> None:
        """Initialize deployer with configuration and AWS clients.

        Args:
            config_path: Path to deployment configuration YAML file
            runner_py_path: Path to runner.py script (default: ./runner.py)
            query_override: Optional SQL query to override config query

        Raises:
            FileNotFoundError: If config file doesn't exist
            KeyError: If required config fields are missing
            ValueError: If no query provided (neither in config nor override)
            yaml.YAMLError: If config file is not valid YAML
            ClientError: If AWS client initialization fails
        """
        self.config = self._load_config(config_path)
        self.config_path = config_path
        self.runner_py_path = runner_py_path or DEFAULT_RUNNER_PATH

        if query_override:
            self.config["query"] = query_override

        self.output_uuid = str(uuid.uuid4())
        self.job_run_id: Optional[str] = None

        self._validate_config()

        self.application_id = self.config["applicationId"]
        self.aws_role = self.config["awsRole"]
        self.aws_region = self.config.get("awsRegion", DEFAULT_AWS_REGION)
        self.aws_profile = self.config.get("awsProfile")
        self.build_path = self.config["buildPath"].rstrip("/")
        self.logs_path = self.config["logsPath"].rstrip("/")
        self.output_prefix = self.config["outputPrefix"].rstrip("/")
        self.local_output_dir = self.config.get(
            "localOutputDir", DEFAULT_LOCAL_OUTPUT_DIR
        )
        self.local_logs_dir = self.config.get("localLogsDir", DEFAULT_LOCAL_LOGS_DIR)

        self.spark_config = {**DEFAULT_SPARK_SUBMIT_CONFIG}
        if "sparkConfig" in self.config:
            self.spark_config.update(self.config["sparkConfig"])

        session_kwargs = {"region_name": self.aws_region}
        if self.aws_profile:
            session_kwargs["profile_name"] = self.aws_profile

        try:
            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client("s3")
            self.emr_client = session.client("emr-serverless")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize AWS clients: {e}") from e

        log("Initialized EMR Serverless Deployer")
        log(f"  Application ID: {self.application_id}")
        log(f"  AWS Region: {self.aws_region}")
        if self.aws_profile:
            log(f"  AWS Profile: {self.aws_profile}")
        log(f"  Output UUID: {self.output_uuid}")
        log(f"  Build Path: {self.build_path}")
        log(f"  Logs Path: {self.logs_path}")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load deployment configuration from YAML file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            Parsed configuration dictionary

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If file is not valid YAML
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            if config is None:
                raise ValueError(f"Configuration file is empty: {config_path}")

            return config
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML in configuration file: {e}") from e

    def _validate_config(self) -> None:
        """Validate that all required configuration fields are present.

        Raises:
            KeyError: If required field is missing
            ValueError: If field value is invalid
        """
        required_fields = [
            "applicationId",
            "awsRole",
            "buildPath",
            "logsPath",
            "outputPrefix",
            "datasets",
        ]

        for field in required_fields:
            if field not in self.config:
                raise KeyError(f"Configuration missing required field: {field}")
            if not self.config[field]:
                raise ValueError(f"Configuration field '{field}' cannot be empty")

        if "query" not in self.config or not self.config.get("query"):
            raise ValueError(
                "No query provided. Either specify 'query' in config file or use --query option."
            )

        if not isinstance(self.config["datasets"], list):
            raise ValueError("'datasets' must be a list")

        if not self.config["datasets"]:
            raise ValueError("'datasets' cannot be empty")

        s3_paths = ["buildPath", "logsPath", "outputPrefix"]
        for path_field in s3_paths:
            path = self.config.get(path_field)
            if path and not path.startswith("s3://"):
                raise ValueError(f"'{path_field}' must be an S3 URI (s3://...): {path}")

    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """Parse S3 URI into bucket and key components.

        Args:
            s3_uri: S3 URI in format s3://bucket/key/path

        Returns:
            Tuple of (bucket, key)

        Raises:
            ValueError: If URI is not valid S3 format
        """
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI (must start with s3://): {s3_uri}")

        parts = s3_uri[5:].split("/", 1)
        bucket = parts[0]

        if not bucket:
            raise ValueError(f"S3 URI missing bucket name: {s3_uri}")

        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    def upload_file_to_s3(self, local_path: str, s3_uri: str) -> str:
        """Upload local file to S3.

        Args:
            local_path: Path to local file
            s3_uri: Target S3 URI

        Returns:
            S3 URI of uploaded file

        Raises:
            FileNotFoundError: If local file doesn't exist
            ClientError: If upload fails
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")

        bucket, key = self._parse_s3_uri(s3_uri)
        log(f"Uploading {local_path} to {s3_uri}")

        try:
            self.s3_client.upload_file(local_path, bucket, key)
            log("  ✓ Upload successful")
            return s3_uri
        except ClientError as e:
            log(f"  ✗ Upload failed: {e}")
            raise RuntimeError(f"Failed to upload {local_path} to {s3_uri}: {e}") from e

    def _read_view_files(self, view_files: List[str]) -> List[Dict[str, str]]:
        """Read SQL view files and return their content.

        Args:
            view_files: List of paths to SQL view files

        Returns:
            List of dicts with 'name' and 'sql' fields

        Raises:
            FileNotFoundError: If any view file doesn't exist
        """
        views = []
        for view_file in view_files:
            log(f"  Reading SQL view file: {view_file}")

            if not os.path.exists(view_file):
                raise FileNotFoundError(f"View file not found: {view_file}")

            try:
                with open(view_file, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                if not sql_content.strip():
                    log(f"  Warning: View file is empty: {view_file}")

                views.append({"name": view_file, "sql": sql_content})
            except Exception as e:
                raise RuntimeError(f"Failed to read view file {view_file}: {e}") from e

        return views

    def generate_execution_config(self) -> Dict[str, Any]:
        """Generate execution configuration from deployment configuration.

        Reads SQL view files and embeds their content. Applies parameter substitution
        to views and query. Extracts only the fields needed for query execution.

        Returns:
            Execution configuration dictionary

        Raises:
            FileNotFoundError: If view files don't exist
            ValueError: If parameter substitution references undefined parameter
        """
        parameters = self.config.get("parameters", {})

        query = substitute_parameters(self.config["query"], parameters)

        execution_config = {
            "datasets": self.config["datasets"],
            "query": query,
            "outputPrefix": self.output_prefix,
        }

        view_files = self.config.get("views", [])
        if view_files:
            if not isinstance(view_files, list):
                raise ValueError("'views' must be a list of file paths")

            views = self._read_view_files(view_files)
            if views:
                for view in views:
                    view["sql"] = substitute_parameters(view["sql"], parameters)
                execution_config["views"] = views

        return execution_config

    def deploy_artifacts(self) -> Dict[str, str]:
        """Upload runner script and execution config to S3.

        Returns:
            Dictionary with S3 URIs of uploaded artifacts: {'runner_py': uri, 'config': uri}

        Raises:
            FileNotFoundError: If runner script doesn't exist
            ClientError: If uploads fail
        """
        log("\n=== Deploying Artifacts ===")

        artifacts = {}

        runner_py_s3 = f"{self.build_path}/runner.py"
        artifacts["runner_py"] = self.upload_file_to_s3(
            self.runner_py_path, runner_py_s3
        )

        execution_config = self.generate_execution_config()

        temp_config_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, encoding="utf-8"
            ) as f:
                json.dump(execution_config, f, indent=2)
                temp_config_path = f.name

            config_s3 = f"{self.build_path}/execution_config_{self.output_uuid}.json"
            artifacts["config"] = self.upload_file_to_s3(temp_config_path, config_s3)

            dataset_names = [d["name"] for d in execution_config["datasets"]]
            log(f"  Generated execution config with datasets: {dataset_names}")

        finally:
            if temp_config_path and os.path.exists(temp_config_path):
                try:
                    os.unlink(temp_config_path)
                except Exception as e:
                    log(
                        f"  Warning: Failed to delete temp file {temp_config_path}: {e}"
                    )

        log("\nAll artifacts deployed successfully")
        return artifacts

    def _build_spark_submit_parameters(self) -> str:
        """Build Spark configuration string for job submission.

        Uses spark_config which merges default config with optional overrides from config file.

        Returns:
            Formatted Spark configuration string
        """
        cfg = self.spark_config
        return (
            f"--conf spark.executor.memory={cfg['executor_memory']} "
            f"--conf spark.executor.cores={cfg['executor_cores']} "
            f"--conf spark.driver.memory={cfg['driver_memory']} "
            f"--conf spark.driver.cores={cfg['driver_cores']} "
            f"--conf spark.dynamicAllocation.enabled=true "
            f"--conf spark.dynamicAllocation.shuffleTracking.enabled=true "
            f"--conf spark.dynamicAllocation.minExecutors={cfg['min_executors']} "
            f"--conf spark.dynamicAllocation.maxExecutors={cfg['max_executors']} "
            f"--conf spark.dynamicAllocation.initialExecutors={cfg['initial_executors']} "
            f"--conf spark.sql.shuffle.partitions={cfg['shuffle_partitions']} "
            f"--conf spark.default.parallelism={cfg['parallelism']}"
        )

    def submit_job(self, artifacts: Dict[str, str]) -> str:
        """Submit Spark job to EMR Serverless.

        Args:
            artifacts: Dictionary with 'runner_py' and 'config' S3 URIs

        Returns:
            Job run ID

        Raises:
            KeyError: If artifacts missing required keys
            ClientError: If job submission fails
        """
        log("\n=== Submitting Job ===")

        if "runner_py" not in artifacts:
            raise KeyError("Artifacts missing 'runner_py' key")
        if "config" not in artifacts:
            raise KeyError("Artifacts missing 'config' key")

        job_name = f"emr-query-{self.output_uuid[:8]}"
        spark_submit_parameters = self._build_spark_submit_parameters()

        job_driver = {
            "sparkSubmit": {
                "entryPoint": artifacts["runner_py"],
                "entryPointArguments": [artifacts["config"], self.output_uuid],
                "sparkSubmitParameters": spark_submit_parameters,
            }
        }

        configuration_overrides = {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": self.logs_path}
            }
        }

        try:
            response = self.emr_client.start_job_run(
                applicationId=self.application_id,
                executionRoleArn=self.aws_role,
                name=job_name,
                jobDriver=job_driver,
                configurationOverrides=configuration_overrides,
            )

            job_run_id = response["jobRunId"]
            self.job_run_id = job_run_id

            log("Job submitted successfully!")
            log(f"  Job Name: {job_name}")
            log(f"  Job Run ID: {job_run_id}")
            log(f"  ARN: {response['arn']}")

            return job_run_id

        except ClientError as e:
            error_msg = f"Failed to submit job: {e}"
            log(error_msg)
            raise RuntimeError(error_msg) from e

    def get_job_status(self, job_run_id: str) -> Dict[str, Any]:
        """Retrieve current job status from EMR Serverless.

        Args:
            job_run_id: EMR job run identifier

        Returns:
            Job run details dictionary

        Raises:
            ClientError: If API call fails
        """
        try:
            response = self.emr_client.get_job_run(
                applicationId=self.application_id, jobRunId=job_run_id
            )
            return response["jobRun"]
        except ClientError as e:
            error_msg = f"Failed to get job status: {e}"
            log(error_msg)
            raise RuntimeError(error_msg) from e

    def monitor_job(
        self, job_run_id: str, poll_interval: int = DEFAULT_POLL_INTERVAL
    ) -> bool:
        """Monitor job execution until completion.

        Polls job status at regular intervals and logs state changes.
        Continues until job reaches a terminal state. Displays Spark UI URL
        when job enters RUNNING state.

        Args:
            job_run_id: EMR job run identifier
            poll_interval: Seconds between status checks

        Returns:
            True if job succeeded, False if failed or cancelled

        Raises:
            ClientError: If status check fails
        """
        log("\n=== Monitoring Job ===")
        log(f"Job Run ID: {job_run_id}")
        log(f"Polling every {poll_interval} seconds...\n")

        previous_state = None
        start_time = time.time()
        spark_ui_shown = False

        while True:
            job_run = self.get_job_status(job_run_id)
            state = job_run["state"]

            if state != previous_state:
                elapsed = int(time.time() - start_time)
                timestamp = time.strftime("%H:%M:%S")
                log(f"[{timestamp}] ({elapsed}s) State: {state}")

                if state == "RUNNING" and not spark_ui_shown:
                    try:
                        dashboard_response = self.emr_client.get_dashboard_for_job_run(
                            applicationId=self.application_id, jobRunId=job_run_id
                        )
                        dashboard_url = dashboard_response.get("url")
                        if dashboard_url:
                            log(f"\n  Spark UI: {dashboard_url}\n")
                    except ClientError as e:
                        log(f"  Note: Could not fetch Spark UI URL: {e}")
                    finally:
                        spark_ui_shown = True

                if state in TERMINAL_STATES:
                    log("\n" + "=" * 50)
                    log(f"Job {state}")
                    log("=" * 50)

                    if "stateDetails" in job_run:
                        log(f"Details: {job_run['stateDetails']}")

                    if state == SUCCESS_STATE:
                        log("\n✓ Job completed successfully!")
                        log(f"Total runtime: {elapsed}s")
                        return True
                    else:
                        log("\n✗ Job failed or was cancelled")
                        return False

                previous_state = state

            time.sleep(poll_interval)

    def download_driver_logs(self) -> str:
        """Download and decompress EMR driver logs from S3.

        Returns:
            Path to downloaded log file

        Raises:
            ValueError: If job_run_id not set
            ClientError: If download fails
        """
        if not self.job_run_id:
            raise ValueError("job_run_id not set - job has not been submitted")

        log("\n=== Downloading Driver Logs ===")

        log_s3_path = (
            f"{self.logs_path}/applications/{self.application_id}/"
            f"jobs/{self.job_run_id}/SPARK_DRIVER/stderr.gz"
        )
        bucket, key = self._parse_s3_uri(log_s3_path)

        log(f"Downloading from: {log_s3_path}")

        os.makedirs(self.local_logs_dir, exist_ok=True)
        local_log_path = os.path.join(
            self.local_logs_dir, f"{self.job_run_id}_driver_stderr.log"
        )

        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            compressed_data = response["Body"].read()
            decompressed_data = gzip.decompress(compressed_data)

            with open(local_log_path, "wb") as f:
                f.write(decompressed_data)

            log(f"✓ Downloaded and decompressed to: {local_log_path}")
            return local_log_path

        except ClientError as e:
            error_msg = f"Failed to download logs: {e}"
            log(f"✗ {error_msg}")
            raise RuntimeError(error_msg) from e

    def display_parquet_results(self, output_path: str) -> None:
        """Read and display parquet results to stdout.

        Args:
            output_path: Local directory containing parquet files

        Raises:
            FileNotFoundError: If output directory doesn't exist
            Exception: If parquet reading fails
        """
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output directory not found: {output_path}")

        try:
            parquet_files = list(Path(output_path).glob("*.parquet"))

            if not parquet_files:
                log("No parquet files found in output directory")
                return

            df = pd.read_parquet(output_path)

            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_rows", None)
            pd.set_option("display.width", None)
            pd.set_option("display.max_colwidth", None)

            print(df.to_string(), file=sys.stdout)

        except Exception as e:
            error_msg = f"Failed to display results: {e}"
            log(f"✗ {error_msg}")
            raise RuntimeError(error_msg) from e

    def display_driver_logs(self, log_path: str) -> None:
        """Read and display driver logs to stderr.

        Args:
            log_path: Path to local log file

        Raises:
            FileNotFoundError: If log file doesn't exist
            Exception: If reading fails
        """
        if not os.path.exists(log_path):
            raise FileNotFoundError(f"Log file not found: {log_path}")

        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log_content = f.read()

            print("\n" + "=" * 80, file=sys.stderr)
            print("DRIVER LOGS", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            print(log_content, file=sys.stderr)
            print("=" * 80 + "\n", file=sys.stderr)

        except Exception as e:
            error_msg = f"Failed to display logs: {e}"
            log(f"✗ {error_msg}")
            raise RuntimeError(error_msg) from e

    def download_results(self) -> str:
        """Download all result files from S3 to local directory.

        Returns:
            Path to local directory containing downloaded files

        Raises:
            ValueError: If no output files found in S3
            ClientError: If download fails
        """
        log("\n=== Downloading Results ===")

        output_s3_path = f"{self.output_prefix}/{self.output_uuid}"
        bucket, prefix = self._parse_s3_uri(output_s3_path)

        log(f"Downloading from: {output_s3_path}")

        local_output_path = os.path.join(self.local_output_dir, self.output_uuid)
        os.makedirs(local_output_path, exist_ok=True)

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix.rstrip("/") + "/"
            )

            if "Contents" not in response or not response["Contents"]:
                raise ValueError(f"No output files found at {output_s3_path}")

            file_count = 0
            total_size = 0

            for obj in response["Contents"]:
                s3_key = obj["Key"]

                rel_path = s3_key[len(prefix.rstrip("/") + "/") :]
                if not rel_path:  # Skip the directory itself
                    continue

                local_file_path = os.path.join(local_output_path, rel_path)

                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                log(f"  Downloading: {rel_path}")
                self.s3_client.download_file(bucket, s3_key, local_file_path)

                file_count += 1
                total_size += obj["Size"]

            log(
                f"✓ Downloaded {file_count} files ({total_size:,} bytes) "
                f"to: {local_output_path}"
            )
            return local_output_path

        except ClientError as e:
            error_msg = f"Failed to download results: {e}"
            log(f"✗ {error_msg}")
            raise RuntimeError(error_msg) from e

    def run(
        self,
        monitor: bool = True,
        display: bool = False,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ) -> Optional[bool]:
        """Execute complete deployment and execution workflow.

        Args:
            monitor: Whether to monitor job status until completion
            display: Whether to display results/logs to stdout/stderr
            poll_interval: Seconds between status checks when monitoring

        Returns:
            True if job succeeded, False if failed, None if not monitored

        Raises:
            Exception: If any step of the workflow fails
        """
        try:
            artifacts = self.deploy_artifacts()

            job_run_id = self.submit_job(artifacts)

            if monitor:
                success = self.monitor_job(job_run_id, poll_interval)

                if success:
                    output_path = self.download_results()

                    if display:
                        self.display_parquet_results(output_path)
                    else:
                        print(output_path, file=sys.stdout)

                    return True
                else:
                    log_path = self.download_driver_logs()

                    if display:
                        self.display_driver_logs(log_path)

                    return False
            else:
                log(f"\nJob submitted. Run ID: {job_run_id}")
                log(
                    f"Monitor manually with: aws emr-serverless get-job-run "
                    f"--application-id {self.application_id} "
                    f"--job-run-id {job_run_id}"
                )
                return None

        except Exception as e:
            log(f"\n✗ Deployment failed: {e}")
            raise


def main() -> None:
    """Main entry point for deployment orchestrator.

    Parses command-line arguments and executes deployment workflow.

    Exit codes:
        0: Success or job submitted without monitoring
        1: Job failed or deployment error
    """
    parser = argparse.ArgumentParser(
        description="Deploy and run Spark jobs on AWS EMR Serverless",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("config", help="Path to query configuration YAML file")
    parser.add_argument(
        "--query",
        default=None,
        help="SQL query to execute (overrides query in config file)",
    )
    parser.add_argument(
        "--runner-py",
        default=None,
        help=f"Path to runner.py file to upload (default: {DEFAULT_RUNNER_PATH})",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Submit job without monitoring (default: monitor enabled)",
    )
    parser.add_argument(
        "--display",
        action="store_true",
        help="Display results (on success) or driver logs (on failure) to stdout/stderr",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Seconds between status checks when monitoring (default: {DEFAULT_POLL_INTERVAL})",
    )

    args = parser.parse_args()

    if args.poll_interval < 1:
        log("Error: --poll-interval must be at least 1 second")
        sys.exit(1)

    try:
        deployer = EMRServerlessDeployer(
            config_path=args.config,
            runner_py_path=args.runner_py,
            query_override=args.query,
        )

        success = deployer.run(
            monitor=not args.no_monitor,
            display=args.display,
            poll_interval=args.poll_interval,
        )

        if success is not None:
            sys.exit(0 if success else 1)
        else:
            sys.exit(0)

    except KeyboardInterrupt:
        log("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        log(f"\nFatal error: {e}")
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
