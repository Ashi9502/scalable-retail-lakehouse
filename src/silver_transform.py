from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import load_config
from src.common.spark import build_spark


def main() -> None:
    cfg = load_config()
    allowed_statuses = cfg["quality"]["allowed_statuses"]

    spark = build_spark("retail-silver-transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

    customers = spark.table("bronze.customers")
    products = spark.table("bronze.products")
    orders = spark.table("bronze.orders")

    customer_window = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )
    customers_clean = (
        customers.filter(F.col("customer_id").isNotNull())
        .withColumn("_rn", F.row_number().over(customer_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    product_window = Window.partitionBy("product_id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )
    products_clean = (
        products.filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 0))
        .withColumn("_rn", F.row_number().over(product_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    order_window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )
    orders_dedup = (
        orders.withColumn("_rn", F.row_number().over(order_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("order_date", F.to_date("order_ts"))
    )

    valid_customer_ids = customers_clean.select("customer_id").distinct()
    valid_product_ids = products_clean.select("product_id").distinct()

    with_fk = (
        orders_dedup.alias("o")
        .join(
            valid_customer_ids.withColumnRenamed("customer_id", "_valid_customer"),
            F.col("o.customer_id") == F.col("_valid_customer"),
            "left",
        )
        .join(
            valid_product_ids.withColumnRenamed("product_id", "_valid_product"),
            F.col("o.product_id") == F.col("_valid_product"),
            "left",
        )
    )

    valid_expr = (
        F.col("order_id").isNotNull()
        & F.col("_valid_customer").isNotNull()
        & F.col("_valid_product").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
        & (F.col("order_amount") >= 0)
        & F.col("status").isin(allowed_statuses)
    )

    valid_orders = with_fk.filter(valid_expr).drop("_valid_customer", "_valid_product")
    rejected_orders = (
        with_fk.filter(~valid_expr)
        .withColumn(
            "_reject_reason",
            F.concat_ws(
                "|",
                F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID")),
                F.when(F.col("_valid_customer").isNull(), F.lit("INVALID_CUSTOMER")),
                F.when(F.col("_valid_product").isNull(), F.lit("INVALID_PRODUCT")),
                F.when(F.col("order_amount") < 0, F.lit("NEGATIVE_AMOUNT")),
                F.when(~F.col("status").isin(allowed_statuses), F.lit("INVALID_STATUS")),
            ),
        )
        .drop("_valid_customer", "_valid_product")
    )

    customers_clean.writeTo("silver.customers").using("iceberg").createOrReplace()
    products_clean.writeTo("silver.products").using("iceberg").createOrReplace()
    valid_orders.writeTo("silver.orders").using("iceberg").partitionedBy(
    F.months("order_ts")
).createOrReplace()
    rejected_orders.writeTo("silver.rejected_orders").using("iceberg").createOrReplace()

    print(f"silver.customers={customers_clean.count():,}")
    print(f"silver.products={products_clean.count():,}")
    print(f"silver.orders={valid_orders.count():,}")
    print(f"silver.rejected_orders={rejected_orders.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
