from __future__ import annotations

from pyspark.sql import functions as F

from src.common.spark import build_spark


def ensure_scd2_customer_dimension(spark) -> None:
    staged = (
        spark.table("silver.customers")
        .select("customer_id", "customer_name", "city", "signup_date", "updated_at")
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("customer_name"), F.lit("")),
                    F.coalesce(F.col("city"), F.lit("")),
                ),
                256,
            ),
        )
    )

    if not spark.catalog.tableExists("gold.dim_customers"):
        initial = (
            staged.withColumn(
                "effective_start", F.coalesce("updated_at", F.current_timestamp())
            )
            .withColumn("effective_end", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        initial.writeTo("gold.dim_customers").using("iceberg").create()
        return

    staged.createOrReplaceTempView("staged_customers")

    spark.sql(
        """
        MERGE INTO gold.dim_customers t
        USING staged_customers s
        ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET
          t.is_current = false,
          t.effective_end = current_timestamp()
        """
    )

    current = (
        spark.table("gold.dim_customers")
        .filter(F.col("is_current") == True)
        .select("customer_id", "record_hash")
    )
    to_insert = (
        staged.alias("s")
        .join(
            current.alias("t"),
            (F.col("s.customer_id") == F.col("t.customer_id"))
            & (F.col("s.record_hash") == F.col("t.record_hash")),
            "left_anti",
        )
        .withColumn(
            "effective_start", F.coalesce("updated_at", F.current_timestamp())
        )
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if to_insert.limit(1).count():
        to_insert.writeTo("gold.dim_customers").append()


def main() -> None:
    spark = build_spark("retail-gold-models")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

    ensure_scd2_customer_dimension(spark)

    products = spark.table("silver.products").select(
        "product_id", "product_name", "category", "unit_price", "updated_at"
    )
    products.writeTo("gold.dim_products").using("iceberg").createOrReplace()

    fact = spark.table("silver.orders").select(
        "order_id",
        "customer_id",
        "product_id",
        "order_ts",
        "order_date",
        "quantity",
        "unit_price",
        "discount_pct",
        "order_amount",
        "status",
        "payment_method",
    )
    fact.writeTo("gold.fct_orders").using("iceberg").partitionedBy(
    F.months("order_ts")
).createOrReplace()

    daily = (
        fact.filter(F.col("status") != "CANCELLED")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("order_amount"), 2).alias("gross_sales"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
        )
    )
    daily.writeTo("gold.daily_sales").using("iceberg").createOrReplace()

    print(f"gold.dim_customers={spark.table('gold.dim_customers').count():,}")
    print(f"gold.dim_products={spark.table('gold.dim_products').count():,}")
    print(f"gold.fct_orders={spark.table('gold.fct_orders').count():,}")
    print(f"gold.daily_sales={spark.table('gold.daily_sales').count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
