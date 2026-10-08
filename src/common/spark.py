from __future__ import annotations

import os
from pyspark.sql import SparkSession

from src.common.config import load_config, project_path


def build_spark(app_name: str) -> SparkSession:
    cfg = load_config()
    spark_cfg = cfg["spark"]
    warehouse = project_path(cfg["paths"]["warehouse"]).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)

    iceberg_version = os.getenv(
        "ICEBERG_VERSION", str(spark_cfg.get("iceberg_version", "1.11.0"))
    )
    package = (
        "org.apache.iceberg:"
        f"iceberg-spark-runtime-3.5_2.12:{iceberg_version}"
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", spark_cfg.get("master", "local[*]")))
        .config("spark.jars.packages", package)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse.as_uri())
        .config("spark.sql.defaultCatalog", "local")
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_cfg.get("shuffle_partitions", 16)),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
