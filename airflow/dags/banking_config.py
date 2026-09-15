import os
from datetime import timedelta

from airflow.models import Variable


def get_setting(name: str, default: str) -> str:
    """Read config from Airflow Variable first, then environment, then default."""
    return Variable.get(name, default_var=os.environ.get(name.upper(), default))


ENV = get_setting("banking_env", "dev")
PROJECT_ID = get_setting("banking_project_id", "dev-gcp-100")
REGION = get_setting("banking_region", "us-east1")
BQ_LOCATION = get_setting("banking_bq_location", "US")
TEMP_BUCKET = get_setting("banking_temp_bucket", "banking-temp-dev")
RUNTIME_SERVICE_ACCOUNT = get_setting("banking_runtime_service_account", "")

TEMP_LOCATION = f"gs://{TEMP_BUCKET}/temp/"
STAGING_LOCATION = f"gs://{TEMP_BUCKET}/staging/"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": int(get_setting("banking_default_retries", "2")),
    "retry_delay": timedelta(minutes=int(get_setting("banking_retry_delay_minutes", "5"))),
    "email_on_failure": False,
}
