# GCP Banking Data Platform 

This project demonstrates an end-to-end banking data engineering architecture on Google Cloud Platform (GCP) for handling both batch and real-time/streaming data.

## Architecture
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/876bb5af-f472-4c48-ada9-976ff7e7bf0a" />

## Data Sources

The project uses two main types of data sources.
1. Batch Source- Cloud SQL MySQL database:
   - Customers: Customer profile and KYC data
   - Accounts: Bank Account details, account type, balance, and status
   - Transactions: Historical Banking transactions
2. Streaming Source- Pub/Sub transaction events:
   - Transaction ID
   - Account ID
   - Customer ID
   - Amount
   - Currency
   - Transaction type
   - Channel
   - Merchant
   - Event timestamp
  
## Data Model
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/619b100a-35f0-4d74-8d12-93a3bc190e3f" />

## Key Services

- Cloud SQL
- Pub/Sub
- Dataflow
- Google Cloud Storage (GCS)
- Dataproc
- PySpark
- BigQuery
- Apache Airflow
- GitHub
- Cloud Scheduler
- Cloud Build
- Looker / Power BI
- Medallion Architecture (Bronze, Silver, Gold)

## Key Techniques involved

- Batch Ingestion
- Real-time streaming ingestion
- CDC or change data capture
- Watermark-based incremental loading
- Metadata-driven ingestion
- Bronze, silver, gold medallion architecture
- PySpark transformation
- Apache Beam pipelines
- BigQuery SQL transformations
- SCD Type 2 dimension modelling
- Fact and Dimension table creation
- Data deduplication
- Partitioning and clustering
- Audit logging
- Dead-letter handling for bad streaming messages
- Pipeline orchestration using Airflow
- CI/CD deployment using Cloud Build and GitHub

## Metadata-driven framework
Built a metadata-driven framework to configure and manage data ingestion pipelines, enabling dynamic table processing, incremental data loads, and improved pipeline maintainability
<img width="779" height="345" alt="image" src="https://github.com/user-attachments/assets/324c8a46-97b6-45cd-93da-a7247df6c459" />


## Dataflow ingestion pipeline 1 (Batch Ingestion)

<img width="943" height="448" alt="image" src="https://github.com/user-attachments/assets/3097ed80-2224-466a-8b47-aca5f307dfe6" />

- [Batch_Ingestion_Python_Code](cloudsql_cdc_pipeline.py)

## Bronze Ingestion Layer

The Bronze layer uses **Dataproc with PySpark** to ingest Parquet data from **GCS into BigQuery**. The pipeline is **metadata-driven**, using source paths, primary keys, and watermark columns from the `table_ingestion_config` metadata table. It applies predefined schemas and **CDC-aware deduplication** to retain the latest record for each primary key, adds a technical `bronze_load_ts` column, and writes the data to BigQuery Bronze tables. Each table is processed independently, with execution details captured in an audit table.

**Flow:**

```text
GCS → Dataproc / PySpark → CDC Deduplication → BigQuery Bronze → Audit Log
```

**Technologies:** Dataproc, PySpark, GCS, BigQuery


## Bronze Ingestion Audit Log
<img width="1907" height="512" alt="image" src="https://github.com/user-attachments/assets/a81d2921-f8fb-4a5e-9076-d7af37c27cf4" />

## PySpark code file
- [Bronze_Ingestion_PySpark_Code](bronze_gcs_to_bq.py)

## Dataflow ingestion pipeline 2 (Streaming Ingestion)
<img width="1747" height="694" alt="image" src="https://github.com/user-attachments/assets/28021591-426b-4cd2-bd19-a96234a405b8" />

