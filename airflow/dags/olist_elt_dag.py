"""
Olist ELT pipeline — daily orchestration.

Flow:
  extract_mssql_to_bronze -> extract_api_to_bronze -> dbt_run_staging
    -> dbt_test_staging -> dbt_run_marts -> dbt_test_marts
    -> dbt_snapshot -> notify_success

Extraction uses Parquet + COPY INTO (~8 min for 1.33M rows).
dbt runs on Databricks SQL Warehouse (fast, native SQL execution).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Paths inside the Airflow container (mounted from the host)
DBT_PROJECT_DIR = "/opt/airflow/dbt_olist"
INGESTION_DIR = "/opt/airflow/ingestion"

# Common dbt invocation flags
DBT_BASE_CMD = f"cd {DBT_PROJECT_DIR} && dbt"


default_args = {
    "owner": "mongezi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="olist_elt_pipeline",
    description="End-to-end ELT: MSSQL + API -> bronze -> dbt staging -> marts -> snapshots",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",  # daily at 03:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["olist", "elt", "dbt", "production"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Extraction — MSSQL -> Databricks bronze (Parquet + COPY INTO)
    # ------------------------------------------------------------------
    extract_mssql = BashOperator(
        task_id="extract_mssql_to_bronze",
        bash_command=f"python {INGESTION_DIR}/mssql_to_bronze_v2.py",
    )

    # ------------------------------------------------------------------
    # 2. Extraction — REST API -> Databricks bronze
    # ------------------------------------------------------------------
    extract_api = BashOperator(
        task_id="extract_api_to_bronze",
        bash_command=f"python {INGESTION_DIR}/api_to_bronze.py",
    )

    # ------------------------------------------------------------------
    # 3. dbt staging build
    # ------------------------------------------------------------------
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_BASE_CMD} run --profiles-dir . --select staging",
    )

    # ------------------------------------------------------------------
    # 4. dbt staging tests
    # ------------------------------------------------------------------
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_BASE_CMD} test --profiles-dir . --select staging",
    )

    # ------------------------------------------------------------------
    # 5. dbt marts build (dims + fact)
    # ------------------------------------------------------------------
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_BASE_CMD} run --profiles-dir . --select marts",
    )

    # ------------------------------------------------------------------
    # 6. dbt marts tests
    # ------------------------------------------------------------------
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT_BASE_CMD} test --profiles-dir . --select marts",
    )

    # ------------------------------------------------------------------
    # 7. dbt snapshot (SCD2 history)
    # ------------------------------------------------------------------
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BASE_CMD} snapshot --profiles-dir .",
    )

    # ------------------------------------------------------------------
    # 8. Notify — placeholder for Slack/email in production
    # ------------------------------------------------------------------
    def _notify(**context):
        run_id = context["run_id"]
        dag_id = context["dag"].dag_id
        print(f"[NOTIFY] Pipeline succeeded. dag_id={dag_id} run_id={run_id}")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify,
    )

    # ------------------------------------------------------------------
    # Dependencies (sequential — MSSQL first, then API, then dbt chain)
    # ------------------------------------------------------------------
    extract_mssql >> extract_api >> dbt_run_staging
    dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_marts
    dbt_run_marts >> dbt_test_marts
    dbt_test_marts >> dbt_snapshot
    dbt_snapshot >> notify_success