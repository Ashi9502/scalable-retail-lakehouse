from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.common.config import load_config, project_path
from src.common.spark import build_spark


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    return parser.parse_args()


def time_call(fn) -> float:
    started = time.perf_counter()
    fn()
    return round(time.perf_counter() - started, 3)


def main() -> None:
    args = parse_args()
    cfg = load_config()
    benchmark_dir = project_path(cfg["paths"]["benchmarks"])
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark("retail-benchmark")
    orders = spark.table("silver.orders")
    products = spark.table("silver.products")

    results = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "orders": orders.count(),
        "products": products.count(),
        "runs": args.runs,
        "tests": {},
    }

    aggregation_times = []
    for _ in range(args.runs):
        aggregation_times.append(
            time_call(
                lambda: (
                    orders.groupBy("order_date")
                    .agg(F.sum("order_amount").alias("sales"))
                    .orderBy(F.col("sales").desc())
                    .collect()
                )
            )
        )

    non_broadcast_times = []
    broadcast_times = []

    for _ in range(args.runs):
        non_broadcast_times.append(
            time_call(
                lambda: (
                    orders.join(
                        products.hint("shuffle_hash"), "product_id", "inner"
                    )
                    .groupBy("category")
                    .agg(F.sum("order_amount"))
                    .collect()
                )
            )
        )

        broadcast_times.append(
            time_call(
                lambda: (
                    orders.join(F.broadcast(products), "product_id", "inner")
                    .groupBy("category")
                    .agg(F.sum("order_amount"))
                    .collect()
                )
            )
        )

    before = statistics.median(non_broadcast_times)
    after = statistics.median(broadcast_times)
    improvement = ((before - after) / before * 100) if before else 0.0

    results["tests"]["daily_aggregation_seconds"] = aggregation_times
    results["tests"]["join_non_broadcast_seconds"] = non_broadcast_times
    results["tests"]["join_broadcast_seconds"] = broadcast_times
    results["summary"] = {
        "aggregation_median_seconds": round(statistics.median(aggregation_times), 3),
        "non_broadcast_join_median_seconds": round(before, 3),
        "broadcast_join_median_seconds": round(after, 3),
        "broadcast_join_improvement_pct": round(improvement, 2),
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = benchmark_dir / f"benchmark_{stamp}.json"
    output.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"benchmark written to {output}")
    spark.stop()


if __name__ == "__main__":
    main()
