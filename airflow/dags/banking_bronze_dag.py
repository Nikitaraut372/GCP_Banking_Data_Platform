import os
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator
)

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-banking-2026-499415"
REGION = "us-central1"

# using existing cluster
CLUSTER_NAME = "cluster-fef3"

# Composer bucket
composer_bucket = os.environ["GCS_BUCKET"]
PYSPARK_MAIN = f"gs://{composer_bucket}/data/dataproc/bronze_gcs_to_bq.py"
logging.info("GCS_PYTHON_SCRIPT = %s", PYSPARK_MAIN)

DEFAULT_ARGS = {
    "owner": "data-engineering"
}

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_bronze_dag",
    description="Ephemeral Dataproc cluster for PySpark ingestion",
    schedule_interval=None,     # Manual / CI-CD trigger
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dataproc", "pyspark", "gcs", "bigquery"],
) as dag:
    
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_MAIN},
        },
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    start_cluster >> submit_pyspark >> stop_cluster