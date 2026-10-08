# AWS Deployment Blueprint

The repository runs locally first. Deploy only after you understand every stage.

```text
Source/files
   |
   v
S3 raw
   |
   v
Glue/EMR Spark
   |
   v
S3 + Iceberg + Glue Catalog
(Bronze/Silver/Gold)
   |
   +--> Redshift Serverless serving layer
   |
   +--> dbt tests/marts
   |
   v
CloudWatch metrics/logs

MWAA orchestrates the workflow.
```

## Spark catalog configuration sketch

Replace the local Hadoop catalog with an Iceberg Glue catalog and S3 warehouse.

```text
spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.warehouse = s3://<bucket>/warehouse/
spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl = org.apache.iceberg.aws.s3.S3FileIO
```

Use the Iceberg runtime/AWS bundle appropriate to the Spark runtime.

## Security

- Use IAM roles, not access keys committed to Git.
- Encrypt S3 and Redshift.
- Restrict bucket policies and security groups.
- Store secrets in Secrets Manager / Airflow connections.
