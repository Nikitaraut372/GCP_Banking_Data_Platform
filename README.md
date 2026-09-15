# Banking Data Platform on Google Cloud

End-to-end teaching project for data engineers that demonstrates a banking data platform on Google Cloud using Cloud Composer, Dataflow, Dataproc, Pub/Sub, Cloud SQL, GCS, and BigQuery.

## Architecture

- Cloud SQL stores operational banking entities.
- Dataflow extracts CDC-style batches from Cloud SQL into GCS as parquet.
- Dataproc loads raw parquet into BigQuery bronze tables with audit logging.
- BigQuery SQL transforms bronze data into silver and gold analytics tables.
- Pub/Sub and Dataflow handle streaming transaction events.
- Cloud Build deploys Airflow DAGs and data assets to the Composer bucket.

## Runtime Configuration

The Airflow DAGs read configuration from Airflow Variables first, then matching environment variables, then defaults.

Recommended Airflow Variables:

| Variable | Example |
| --- | --- |
| `banking_env` | `dev` |
| `banking_project_id` | `dev-gcp-100` |
| `banking_region` | `us-east1` |
| `banking_bq_location` | `US` |
| `banking_temp_bucket` | `banking-temp-dev` |
| `banking_cloudsql_instance_connection_name` | `dev-gcp-100:us-east1:mysql-instance` |
| `banking_cloudsql_database` | `banking_db` |
| `banking_cloudsql_user` | `myuser` |
| `banking_cloudsql_password_secret` | `banking-cloudsql-password` |
| `banking_cdc_raw_base` | `gs://banking-raw-dev-100/cloudsql/` |
| `banking_streaming_subscription` | `projects/dev-gcp-100/subscriptions/banking-transactions-sub` |
| `banking_streaming_output_table` | `dev-gcp-100:banking_bronze.bronze_streaming_transactions` |
| `banking_streaming_deadletter_topic` | `projects/dev-gcp-100/topics/banking-transactions-deadletter` |

## Secret Manager

Store the Cloud SQL password in Secret Manager instead of source code:

```bash
printf '%s' 'your-password' | gcloud secrets create banking-cloudsql-password --data-file=-
gcloud secrets add-iam-policy-binding banking-cloudsql-password \
  --member="serviceAccount:YOUR_COMPOSER_OR_DATAFLOW_SERVICE_ACCOUNT" \
  --role="roles/secretmanager.secretAccessor"
```

## Local Syntax Check

Run this before deploying:

```bash
python -m compileall airflow pubsub
```

## Pub/Sub Producer

Run the sample event producer with explicit parameters:

```bash
python pubsub/banking_transaction_producer.py \
  --project-id dev-gcp-100 \
  --topic-id banking-transactions-topic \
  --events-per-second 2 \
  --max-events 100
```

Use `--max-events 0` to run continuously.

## Production Notes

- Do not commit credentials, local environment files, or generated caches.
- Keep project, region, bucket, and table names outside source code through Airflow Variables.
- Monitor `banking_metadata.ingestion_audit_log` after each bronze run.
- Review Cloud Dataflow dead-letter messages before replaying failed streaming records.
- Use separate dev and prod Composer environments and Secret Manager secrets.
