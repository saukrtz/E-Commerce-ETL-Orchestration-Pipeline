# E-Commerce ETL Orchestration Pipeline 

This repository contains a production-style, daily ETL pipeline for e-commerce transaction data. The pipeline is orchestrated using **Apache Airflow** and deployed on **Google Cloud Platform (Cloud Composer)**, with data transformations loading into **BigQuery**.

## Architecture & Tech Stack
* **Orchestrator:** Apache Airflow (Cloud Composer)
* **Data Warehouse:** Google BigQuery
* **Processing:** Python (CSV/Logic module)
* **Storage:** Google Cloud Storage (GCS)

## Project Structure
The project is strictly modularized to separate orchestration from business logic:

* `etl_transactions_pipeline.py`: The main Airflow DAG. Uses `TaskGroups` to define Extract, Transform, and Load stages.
* `transaction_logic.py`: Pure Python business logic. Decoupled from the DAG to enable unit testing and keep the orchestrator lightweight.
* `bigquery_queries.sql`: Optimized SQL scripts for downstream reporting.
* `answers.txt`: Architectural documentation covering scaling and GCP deployment.

## Key Engineering Features
* **Idempotent Execution:** Intermediate files use execution-date-scoped paths to ensure reruns don't duplicate data.
* **Optimized State Transfer:** Passes file paths through XCom rather than heavy DataFrames to prevent database bloat.
* **TaskGroups over SubDAGs:** Eliminates worker deadlocks by utilizing UI-level grouping.
* **SLA Monitoring:** Includes a 1-hour SLA to catch performance regressions.
* **Partitioning & Clustering:** BigQuery tables are partitioned by `transaction_date` and clustered by `category` for cost-efficient querying.

## Deployment (Cloud Composer)
1. Upload `ecommerce_transactions.csv` to your GCS data bucket.
2. Update `RAW_CSV_PATH` in the DAG file.
3. Drop `etl_transactions_pipeline.py` and `transaction_logic.py` into the `/dags` folder.
4. Run `pip install -r requirements.txt` for local testing.

## Installation
* pip install -r requirements.txt

## System Architecture

```mermaid
graph TD
    GCS[Google Cloud Storage <br/> raw CSV files] -->|File Sensor / Read| Composer
    
    subgraph GCP Environment
        Composer[Cloud Composer / Airflow <br/> Orchestration & Compute]
        BQ_Stage[(BigQuery <br/> ecomm_staging)]
        BQ_Report[(BigQuery <br/> ecomm_reporting)]
    end

    Composer -->|1. Stages Raw Data| BQ_Stage
    Composer -->|2. Local Python Transforms| Composer
    Composer -->|3. Loads Aggregated Data| BQ_Report
