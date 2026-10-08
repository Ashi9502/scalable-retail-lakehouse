from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.spark import build_spark


@dataclass
class QualityResult:
    bronze_orders: int
    silver_orders: int
    rejected_orders: int
    rejected_ratio: float
    duplicate_order_ids: int
    null_order_ids: int
    negative_amounts: int
    orphan_customers: int
    orphan_products: int
    passed: bool


def main() -> None:
    cfg = load_config()
    max_rejected_ratio = float(cfg["quality"]["max_rejected_ratio"])
    max_duplicates = int(cfg["quality"]["max_duplicate_order_ids"])

    spark = build_spark("retail-quality-gate")

    bronze = spark.table("bronze.orders")
    orders = spark.table("silver.orders")
    rejected = spark.table("silver.rejected_orders")
    customers = spark.table("silver.customers").select("customer_id")
    products = spark.table("silver.products").select("product_id")

    bronze_count = bronze.count()
    silver_count = orders.count()
    rejected_count = rejected.count()

    duplicate_ids = (
        orders.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    null_ids = orders.filter(F.col("order_id").isNull()).count()
    negatives = orders.filter(F.col("order_amount") < 0).count()

    orphan_customers = (
        orders.select("customer_id")
        .distinct()
        .join(customers, "customer_id", "left_anti")
        .count()
    )
    orphan_products = (
        orders.select("product_id")
        .distinct()
        .join(products, "product_id", "left_anti")
        .count()
    )

    ratio = rejected_count / bronze_count if bronze_count else 0.0
    passed = (
        ratio <= max_rejected_ratio
        and duplicate_ids <= max_duplicates
        and null_ids == 0
        and negatives == 0
        and orphan_customers == 0
        and orphan_products == 0
        and silver_count + rejected_count == bronze_count
    )

    result = QualityResult(
        bronze_orders=bronze_count,
        silver_orders=silver_count,
        rejected_orders=rejected_count,
        rejected_ratio=ratio,
        duplicate_order_ids=duplicate_ids,
        null_order_ids=null_ids,
        negative_amounts=negatives,
        orphan_customers=orphan_customers,
        orphan_products=orphan_products,
        passed=passed,
    )
    print(json.dumps(asdict(result), indent=2))
    spark.stop()

    if not passed:
        raise SystemExit("QUALITY GATE FAILED")


if __name__ == "__main__":
    main()
