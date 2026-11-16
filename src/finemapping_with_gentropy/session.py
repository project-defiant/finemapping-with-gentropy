"""Local Spark + Hail session able to read the LD matrices."""

from __future__ import annotations

import os
import sys

from gentropy.common.session import Session

from finemapping_with_gentropy.config import LDSource

# Hadoop 3.3.4 is the version bundled with pyspark 3.5; hadoop-aws must match it.
_S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


def make_session(source: LDSource, driver_memory: str = "12g") -> Session:
    """Start a local session that can read the LD matrices of `source`.

    The public Pan-UKBB bucket is read anonymously through the Hadoop S3A connector, so no
    cloud account is needed. The Open Targets bucket is read through the GCS connector with
    application-default credentials (`gcloud auth application-default login`).

    Args:
        source (LDSource): Where the LD matrices are read from.
        driver_memory (str): Spark driver memory; everything runs in the driver locally.

    Returns:
        Session: gentropy session with Hail started.
    """
    # Spark workers must run the interpreter of the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark_conf = {
        "spark.driver.memory": driver_memory,
        "spark.kryoserializer.buffer.max": "500m",
        "spark.driver.maxResultSize": "5g",
    }
    if source is LDSource.PUBLIC:
        return Session(
            start_hail=True,
            write_mode="overwrite",
            output_partitions=1,
            extended_spark_conf={
                **spark_conf,
                "spark.jars.packages": _S3A_PACKAGES,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
                ),
                "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
            },
        )
    return Session(
        start_hail=True,
        write_mode="overwrite",
        output_partitions=1,
        add_gcs_connector=True,
        # The default (COMPUTE_ENGINE) only works on GCP virtual machines.
        gcs_configuration={"auth_type": "APPLICATION_DEFAULT"},
        extended_spark_conf=spark_conf,
    )
