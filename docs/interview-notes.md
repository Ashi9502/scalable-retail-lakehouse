# Interview Defense Notes

## Spark
- What creates a shuffle?
- Why can a broadcast join help here?
- What happens if the product dimension becomes too large to broadcast?
- Why partition the fact data by date?
- Why is over-partitioning harmful?
- How would you inspect an execution plan?

## Iceberg
- What does Iceberg add beyond Parquet files?
- How do snapshots help?
- Why do schema and partition evolution matter?
- What does an Iceberg catalog do?

## Reliability
- What makes a load idempotent?
- How would you store a watermark?
- What happens when an Airflow task retries?
- How would you backfill one day without rebuilding all history?
- Why should a quality gate fail before Gold publication?

## Modeling
- Fact vs dimension table?
- Why SCD2 for customer attributes?
- Natural vs surrogate keys?
- What grain does `fct_orders` have?

## AWS
- Why Glue Catalog?
- Glue vs EMR?
- Why Redshift for serving?
- What IAM permissions would Spark need?
- How would you monitor failures and costs?
