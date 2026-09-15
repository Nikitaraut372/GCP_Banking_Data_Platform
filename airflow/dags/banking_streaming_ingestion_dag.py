import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from pendulum import datetime

from banking_config import (
    DEFAULT_ARGS,
    PROJECT_ID,
    REGION,
    RUNTIME_SERVICE_ACCOUNT,
    STAGING_LOCATION,
    TEMP_LOCATION,
    get_setting,
)


composer_bucket = os.environ["GCS_BUCKET"]
STREAMING_PIPELINE_PY = f"gs://{composer_bucket}/data/dataflow/streaming_transactions_pipeline.py"
logging.info("STREAMING_PIPELINE_PY = %s", STREAMING_PIPELINE_PY)

INPUT_SUBSCRIPTION = get_setting(
    "banking_streaming_subscription",
    f"projects/{PROJECT_ID}/subscriptions/banking-transactions-sub",
)
OUTPUT_TABLE = get_setting(
    "banking_streaming_output_table",
    f"{PROJECT_ID}:banking_bronze.bronze_streaming_transactions",
)
DEADLETTER_TOPIC = get_setting(
    "banking_streaming_deadletter_topic",
    f"projects/{PROJECT_ID}/topics/banking-transactions-deadletter",
)


with DAG(
    dag_id="banking_streaming_ingestion_dag",
    description="Start Dataflow streaming pipeline for banking transactions",
    start_date=None,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "streaming", "dataflow"],
) as dag:
    start = EmptyOperator(task_id="start")

    start_streaming_job = BeamRunPythonPipelineOperator(
        task_id="start_streaming_dataflow_job",
        runner=BeamRunnerType.DataflowRunner,
        py_file=STREAMING_PIPELINE_PY,
        py_interpreter="python3",
        py_system_site_packages=True,
        py_options=[],
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
            "save_main_session": True,
            "input_subscription": INPUT_SUBSCRIPTION,
            "output_table": OUTPUT_TABLE,
            "deadletter_topic": DEADLETTER_TOPIC,
            **(
                {"service_account_email": RUNTIME_SERVICE_ACCOUNT}
                if RUNTIME_SERVICE_ACCOUNT
                else {}
            ),
        },
        py_requirements=[],
        dataflow_config={
            "location": REGION,
            "job_name": "banking-streaming-transactions",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> start_streaming_job >> end
