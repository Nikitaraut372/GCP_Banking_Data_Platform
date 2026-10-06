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

## Data Processing & Medallion Architecture

After CDC ingestion, the data is landed in the **GCS Raw layer** in Parquet format. The raw data is then processed using **Dataproc with PySpark** and loaded into the BigQuery Bronze layer.

### Processing Flow

```text
Cloud SQL
    ↓
Dataflow
    ↓
GCS Raw / Landing
    ↓
Dataproc + PySpark
    ↓
BigQuery Bronze
    ↓
BigQuery Silver
    ↓
BigQuery Gold
    ↓
BI / Reporting
```

### Bronze Layer

The Bronze layer preserves the source data with **minimal technical processing**. PySpark applies the required schema, performs basic technical validation, adds ingestion metadata, and handles CDC-aware deduplication where required.

The objective of the Bronze layer is to retain a reliable and traceable representation of the ingested source data without applying major business transformations.

### Silver Layer

Processing includes:

* Data cleaning and null handling
* Data type standardization
* Data-quality validation
* Deduplication
* Joins and enrichment
* Business transformations
* Dimension and fact modelling
* SCD Type 2 processing for applicable dimensions

The Silver layer provides trusted, analytics-ready data for downstream consumption.

### Gold Layer – Business & Reporting Data

The Gold layer contains **business-ready and reporting-oriented datasets** created from the processed Silver layer. These tables apply business rules and aggregations to support analytics and BI reporting.

### Gold Tables

| Gold Table                  | Purpose                                                                     |
| --------------------------- | --------------------------------------------------------------------------- |
| `customer_360`              | Customer-level view combining customer, account and transaction information |
| `daily_account_balance`     | Daily snapshot of current account balances                                  |
| `daily_transaction_summary` | Daily transaction count and transaction amount by channel                   |
| `fraud_indicators`          | Identifies high-value transactions based on a defined transaction threshold |

<img width="334" height="82" alt="image" src="https://github.com/user-attachments/assets/5d179570-aa17-4a97-b8ad-9d5c1eb9b1e3" />


### Gold Processing

```text
Silver
  ├── dim_customer
  ├── dim_account
  └── fact_transactions
          ↓
       Gold SQL
          ↓
  ┌─────────────────────────────┐
  │ customer_360                │
  │ daily_account_balance       │
  │ daily_transaction_summary   │
  │ fraud_indicators            │
  └─────────────────────────────┘
          ↓
     Looker / BI
```

### Example Gold Table

**Customer 360**

The `customer_360` table provides a consolidated customer-level view with metrics such as total accounts, total balance, KYC status, and last transaction timestamp.

<img width="706" height="326" alt="image" src="https://github.com/user-attachments/assets/40640260-1752-47e1-baad-afc28e0c035d" />


### Gold Analytics

The Gold layer is designed for downstream BI consumption and can be connected to Looker to create dashboards for:

* Customer 360 analysis
* Account balance trends
* Transaction volume and amount by channel
* High-value transaction monitoring

## Banking Dashboard

The banking overview dashboard was created using Looker Studio (Google Data Studio) and connected to the processed banking data in BigQuery

<img width="918" height="646" alt="image" src="https://github.com/user-attachments/assets/18bb5111-447e-413a-936a-9a7aca61a0fe" />
<img width="574" height="335" alt="image" src="https://github.com/user-attachments/assets/54e92ff9-0f93-4864-b98b-ec32b679e5ab" />
<img width="570" height="329" alt="image" src="https://github.com/user-attachments/assets/e286a84d-636f-4bd8-afda-ca01b4f474ee" />

<img width="590" height="390" alt="image" src="https://github.com/user-attachments/assets/f516fcd3-2016-419f-a4db-3a91dcf20a8b" />

## Orchestration using Cloud Composer
<img width="959" height="452" alt="image" src="https://github.com/user-attachments/assets/7c974f19-d1f4-4f83-ab86-1dc802315b30" />
<img width="959" height="452" alt="image" src="https://github.com/user-attachments/assets/48d60302-90c9-44b3-b78d-7ac464c40c2f" />



