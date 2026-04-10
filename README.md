# E-Commerce ETL Orchestration Pipeline 🚀

This repository contains a production-style, daily ETL pipeline for e-commerce transaction data. [cite_start]The pipeline is orchestrated using **Apache Airflow** and deployed on **Google Cloud Platform (Cloud Composer)**, with data transformations loading into **BigQuery**[cite: 1, 4, 7].

## 🏗️ Architecture & Tech Stack
* [cite_start]**Orchestrator:** Apache Airflow (Cloud Composer) [cite: 2, 7]
* [cite_start]**Data Warehouse:** Google BigQuery [cite: 2, 6]
* [cite_start]**Processing:** Python (CSV/Logic module) [cite: 27, 39]
* [cite_start]**Storage:** Google Cloud Storage (GCS) [cite: 64]

## 📂 Project Structure
[cite_start]The project is strictly modularized to separate orchestration from business logic[cite: 38, 39]:

* `etl_transactions_pipeline.py`: The main Airflow DAG. [cite_start]Uses `TaskGroups` to define Extract, Transform, and Load stages[cite: 24, 25, 70].
* `transaction_logic.py`: Pure Python business logic. [cite_start]Decoupled from the DAG to enable unit testing and keep the orchestrator lightweight[cite: 39, 71].
* [cite_start]`bigquery_queries.sql`: Optimized SQL scripts for downstream reporting[cite: 55, 72].
* [cite_start]`answers.txt`: Architectural documentation covering scaling and GCP deployment[cite: 73].

## ✨ Key Engineering Features
* [cite_start]**Idempotent Execution:** Intermediate files use execution-date-scoped paths to ensure reruns don't duplicate data[cite: 16, 42].
* [cite_start]**Optimized State Transfer:** Passes file paths through XCom rather than heavy DataFrames to prevent database bloat[cite: 41, 42].
* [cite_start]**TaskGroups over SubDAGs:** Eliminates worker deadlocks by utilizing UI-level grouping[cite: 25, 36].
* [cite_start]**SLA Monitoring:** Includes a 1-hour SLA to catch performance regressions[cite: 44].
* [cite_start]**Partitioning & Clustering:** BigQuery tables are partitioned by `transaction_date` and clustered by `category` for cost-efficient querying[cite: 59, 60].

## 🚀 Deployment (Cloud Composer)
1. [cite_start]Upload `ecommerce_transactions.csv` to your GCS data bucket[cite: 12, 19].
2. [cite_start]Update `RAW_CSV_PATH` in the DAG file[cite: 27, 28].
3. [cite_start]Drop `etl_transactions_pipeline.py` and `transaction_logic.py` into the `/dags` folder[cite: 64, 71].
4. Run `pip install -r requirements.txt` for local testing.

## 🗺️ System Architecture

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
