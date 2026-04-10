import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from transaction_logic import (
    validate_schema,
    compute_net_revenue,
    compute_category_summary,
    compute_return_rate,
    update_run_log,
)

RAW_CSV_PATH = "/home/airflow/gcs/data/ecommerce_transactions.csv"
TMP_DIR = "/tmp/ecomm_etl"

def _tmp(execution_date: str, filename: str) -> str:
    day_dir = os.path.join(TMP_DIR, execution_date)
    os.makedirs(day_dir, exist_ok=True)
    return os.path.join(day_dir, filename)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    description="Daily ETL pipeline for transactions",
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["ecommerce", "etl"],
) as dag:

    with TaskGroup("extract") as extract_group:
        
        validate_schema_task = PythonOperator(
            task_id="validate_schema",
            python_callable=validate_schema,
            op_kwargs={"csv_path": RAW_CSV_PATH},
        )

        load_to_staging_task = BashOperator(
            task_id="load_to_staging",
            bash_command="echo 'Loading to ecomm_staging.raw_transactions'",
        )

        validate_schema_task >> load_to_staging_task

    with TaskGroup("transform") as transform_group:

        compute_net_revenue_task = PythonOperator(
            task_id="compute_net_revenue",
            python_callable=compute_net_revenue,
            op_kwargs={
                "csv_path": RAW_CSV_PATH,
                "output_path": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}/enriched_transactions.csv",
            },
        )

        compute_category_summary_task = PythonOperator(
            task_id="compute_category_summary",
            python_callable=compute_category_summary,
            op_kwargs={
                "enriched_csv_path": "{{ ti.xcom_pull(task_ids='transform.compute_net_revenue', key='enriched_csv_path') }}",
                "output_path": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}/category_summary.json",
            },
        )

        compute_return_rate_task = PythonOperator(
            task_id="compute_return_rate",
            python_callable=compute_return_rate,
            op_kwargs={
                "csv_path": RAW_CSV_PATH,
                "output_path": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}/return_rates.json",
            },
        )

        compute_net_revenue_task >> compute_category_summary_task
        compute_net_revenue_task >> compute_return_rate_task

    with TaskGroup("load") as load_group:

        write_to_reporting_table_task = BashOperator(
            task_id="write_to_reporting_table",
            bash_command="echo 'Writing to ecomm_reporting.category_summary'",
        )

        update_run_log_task = PythonOperator(
            task_id="update_run_log",
            python_callable=update_run_log,
            op_kwargs={"csv_path": RAW_CSV_PATH},
        )

        write_to_reporting_table_task >> update_run_log_task

    extract_group >> transform_group >> load_group