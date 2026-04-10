# E-Commerce ETL Orchestration Pipeline 🚀

This repository contains a production-style, daily ETL pipeline for e-commerce transaction data. The pipeline is orchestrated using **Apache Airflow** and deployed on **Google Cloud Platform (Cloud Composer)**, with data transformations loading into **BigQuery**.

## 🏗️ Architecture & Tech Stack
* **Orchestrator:** Apache Airflow (Cloud Composer)
* **Data Warehouse:** Google BigQuery
* **Processing:** Python (Pandas/CSV)
* **Storage:** Google Cloud Storage (GCS)

## 📂 Project Structure
The project is strictly modularized to separate orchestration from business logic:

* `etl_transactions_pipeline.py`: The main Airflow DAG. Uses the TaskFlow API and `TaskGroups` to clearly define the Extract, Transform, and Load stages.
* `transaction_logic.py`: The pure Python business logic. Decoupled from the DAG to enable independent unit testing and keep the orchestrator lightweight.
* `bigquery_queries.sql`: Highly optimized BigQuery SQL scripts for downstream reporting, utilizing `COUNTIF` and partition pruning.
* `answers.txt`: Architectural documentation detailing Executor scaling strategies, Composer deployment, and GCP cost management.

## ✨ Key Engineering Features
* **Idempotent Execution:** Intermediate files are written to execution-date-scoped (`{{ ds }}`) paths. Rerunning a historical date will safely overwrite without duplicating data.
* **Optimized State Transfer:** Prevents database bloat by passing lightweight file paths through XCom rather than serializing heavy DataFrames.
* **TaskGroups over SubDAGs:** Eliminates worker deadlocks and performance overhead by utilizing UI-level grouping.
* **Parallel Processing:** Independent transformations (like category summaries and return rates) are wired to execute concurrently.
* **Data Warehouse Optimization:** The downstream BigQuery table is partitioned by `transaction_date` and clustered by `category` to drastically reduce query costs on large datasets.

## 🚀 Deployment (Cloud Composer)
1. Upload `ecommerce_transactions.csv` to your designated GCS data bucket.
2. Update the `RAW_CSV_PATH` in the DAG file to match your environment.
3. Drop `etl_transactions_pipeline.py` and `transaction_logic.py` into the `/dags` folder of your Cloud Composer bucket.
4. The pipeline is scheduled to run `@daily` with an SLA of 1 hour.

## Installation
* pip install -r requirements.txt

## 🗺️ System Architecture

The pipeline orchestrates the flow of data from raw flat files in Cloud Storage, through Python-based transformation workers, and finally into an optimized BigQuery data warehouse.

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


graph LR
    subgraph 1. Extract Group
        A[validate_schema] --> B[load_to_staging]
    end
    
    subgraph 2. Transform Group
        C[compute_net_revenue] --> D[compute_category_summary]
        C --> E[compute_return_rate]
    end
    
    subgraph 3. Load Group
        F[write_to_reporting_table] --> G[update_run_log]
    end

    1. Extract Group --> 2. Transform Group
    2. Transform Group --> 3. Load Group

