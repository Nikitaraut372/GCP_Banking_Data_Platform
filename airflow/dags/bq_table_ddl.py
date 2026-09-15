import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from banking_config import BQ_LOCATION, DEFAULT_ARGS, PROJECT_ID


METADATA_DATASET = "banking_metadata"
TABLE_DETAILS = "table_details"
COMPOSER_BUCKET = os.environ["GCS_BUCKET"]
DDL_BASE_PATH = "data/bigquery/table_ddl"


def fetch_tables_to_create(**context):
    bq = BigQueryHook(use_legacy_sql=False, location=BQ_LOCATION)
    query = f"""
        SELECT dataset, table
        FROM `{PROJECT_ID}.{METADATA_DATASET}.{TABLE_DETAILS}`
        WHERE create_table = TRUE
    """

    records = bq.get_records(query)
    context["ti"].xcom_push(key="tables_to_create", value=records)


def create_tables_from_gcs_ddl(**context):
    tables = context["ti"].xcom_pull(key="tables_to_create") or []
    if not tables:
        return

    bq = BigQueryHook(use_legacy_sql=False, location=BQ_LOCATION)
    gcs = GCSHook()

    for dataset, table in tables:
        ddl_object_path = f"{DDL_BASE_PATH}/{dataset}/{table}.sql"
        ddl_sql = gcs.download(
            bucket_name=COMPOSER_BUCKET,
            object_name=ddl_object_path,
        ).decode("utf-8")
        bq.run(ddl_sql)

        update_sql = f"""
            UPDATE `{PROJECT_ID}.{METADATA_DATASET}.{TABLE_DETAILS}`
            SET create_timestamp = CURRENT_TIMESTAMP()
            WHERE dataset = @dataset
              AND table = @table
        """
        bq.insert_job(
            configuration={
                "query": {
                    "query": update_sql,
                    "useLegacySql": False,
                    "parameterMode": "NAMED",
                    "queryParameters": [
                        {
                            "name": "dataset",
                            "parameterType": {"type": "STRING"},
                            "parameterValue": {"value": dataset},
                        },
                        {
                            "name": "table",
                            "parameterType": {"type": "STRING"},
                            "parameterValue": {"value": table},
                        },
                    ],
                }
            },
            project_id=PROJECT_ID,
            location=BQ_LOCATION,
        )


with DAG(
    dag_id="bq_metadata_driven_table_creation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bigquery", "ddl", "metadata", "banking"],
) as dag:
    fetch_tables = PythonOperator(
        task_id="fetch_tables_to_create",
        python_callable=fetch_tables_to_create,
    )

    create_tables = PythonOperator(
        task_id="create_tables_from_gcs_ddl",
        python_callable=create_tables_from_gcs_ddl,
    )

    fetch_tables >> create_tables
