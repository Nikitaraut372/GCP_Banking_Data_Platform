from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

from banking_config import DEFAULT_ARGS


with DAG(
    dag_id="banking_master_dag",
    description="Master DAG orchestrating Banking Data Platform",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "master", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        trigger_dag_id="banking_ingestion_dag",
        wait_for_completion=True,
        poke_interval=60,
        execution_timeout=timedelta(minutes=30),
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_dag",
        trigger_dag_id="banking_bronze_dag",
        wait_for_completion=True,
        poke_interval=60,
        execution_timeout=timedelta(hours=2),
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="banking_silver_dag",
        wait_for_completion=True,
        poke_interval=60,
        execution_timeout=timedelta(hours=1),
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="banking_gold_dag",
        wait_for_completion=True,
        poke_interval=60,
        execution_timeout=timedelta(hours=1),
        allowed_states=["success"],
        failed_states=["failed"],
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_ingestion >> trigger_bronze >> trigger_silver >> trigger_gold >> end
