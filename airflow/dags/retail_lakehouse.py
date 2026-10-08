from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_ROOT = os.environ.get(
    "RETAIL_PROJECT_ROOT",
    "/opt/airflow/scalable-retail-lakehouse",
)
COMMON = f"cd {PROJECT_ROOT} && source .venv/bin/activate && "

with DAG(
    dag_id="retail_lakehouse",
    description="Bronze/Silver/Gold retail lakehouse with quality gate and dbt serving marts",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "ashi",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["data-engineering", "spark", "iceberg", "dbt"],
) as dag:
    generate = BashOperator(
        task_id="generate_data",
        bash_command=COMMON
        + "python -m src.generate_data --orders 500000 --customers 50000 --products 5000",
    )
    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=COMMON + "python -m src.bronze_ingest",
    )
    silver = BashOperator(
        task_id="silver_transform",
        bash_command=COMMON + "python -m src.silver_transform",
    )
    quality = BashOperator(
        task_id="quality_gate",
        bash_command=COMMON + "python -m src.quality_gate",
    )
    gold = BashOperator(
        task_id="gold_models",
        bash_command=COMMON + "python -m src.gold_models",
    )
    export = BashOperator(
        task_id="export_silver",
        bash_command=COMMON + "python -m src.export_silver",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=COMMON + "cd dbt && dbt build --profiles-dir .",
    )
    benchmark = BashOperator(
        task_id="benchmark",
        bash_command=COMMON + "python -m src.benchmark --runs 3",
    )

    generate >> bronze >> silver >> quality >> gold >> export >> dbt_build >> benchmark
