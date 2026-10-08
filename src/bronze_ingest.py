from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import load_config, project_path
from src.common.spark import build_spark


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", LongType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("order_ts", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("order_amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def enrich(df):
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def write_bronze(df, table: str) -> None:
    # Bronze preserves landed rows; business-key logic happens downstream.
    df.writeTo(table).using("iceberg").createOrReplace()


def main() -> None:
    cfg = load_config()
    raw = project_path(cfg["paths"]["raw"])
    spark = build_spark("retail-bronze-ingestion")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    datasets = [
        ("customers", CUSTOMER_SCHEMA),
        ("products", PRODUCT_SCHEMA),
        ("orders", ORDER_SCHEMA),
    ]

    for name, schema in datasets:
        path = raw / f"{name}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}. Run generate_data.py first.")
        df = spark.read.option("header", True).schema(schema).csv(str(path))
        write_bronze(enrich(df), f"bronze.{name}")
        print(f"bronze.{name}: {df.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
