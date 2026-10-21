"""Spark session factory."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(env: str, app_name: str, hive: bool = False) -> SparkSession:
    """Create or retrieve a SparkSession for the given environment."""
    builder = SparkSession.builder.appName(app_name)

    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if env == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.warehouse.dir", "/work/data/spark-warehouse")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dderby.system.home=/work/data/derby",
            )
        )
        if hive:
            builder = builder.enableHiveSupport()
    else:
        # gcp: YARN decides master; Dataproc sets it via spark-submit
        builder = builder.config("spark.sql.shuffle.partitions", "200")

    return builder.getOrCreate()
