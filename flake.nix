{
  description = "Python environment with PySpark, AWS CLI, and S3/Parquet support";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        pythonEnv = pkgs.python311.withPackages (ps: with ps; [
          pyspark
          pyarrow
          boto3
          s3fs
          pandas
          fastparquet
          python-dotenv
          pyyaml
        ]);

      in
      {
        packages.default = pkgs.writeShellScriptBin "emr-query" ''
          export PYSPARK_PYTHON="${pythonEnv}/bin/python"
          export PYSPARK_DRIVER_PYTHON="${pythonEnv}/bin/python"
          export JAVA_HOME="${pkgs.jdk17}"
          export SPARK_HOME="${pythonEnv}/${pythonEnv.sitePackages}/pyspark"
          export SPARK_EXTRA_CLASSPATH="${pkgs.hadoop}/share/hadoop/tools/lib/*"

          exec ${pythonEnv}/bin/python ${./deploy.py} --runner-py ${./runner.py} "$@"
        '';

        devShells.default = pkgs.mkShell {
          buildInputs = [
            pythonEnv
            pkgs.awscli2
            pkgs.jdk17
            pkgs.hadoop
          ];

          shellHook = ''
            export PYSPARK_PYTHON="${pythonEnv}/bin/python"
            export PYSPARK_DRIVER_PYTHON="${pythonEnv}/bin/python"
            export JAVA_HOME="${pkgs.jdk17}"
            export SPARK_HOME="${pythonEnv}/${pythonEnv.sitePackages}/pyspark"

            # Add Hadoop AWS JARs for S3 support
            export SPARK_EXTRA_CLASSPATH="${pkgs.hadoop}/share/hadoop/tools/lib/*"

            echo "Python environment with PySpark ready!"
            echo "Python: $(python --version)"
            echo "PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"
            echo "AWS CLI: $(aws --version)"
            echo ""
            echo "S3 and Parquet libraries available:"
            echo "  - boto3 (AWS SDK)"
            echo "  - s3fs (S3 filesystem)"
            echo "  - pyarrow (Parquet support)"
            echo "  - fastparquet (Alternative Parquet library)"
          '';
        };
      }
    );
}
