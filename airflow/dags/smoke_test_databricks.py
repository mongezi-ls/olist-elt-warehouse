"""
Smoke test: verify Airflow can reach Databricks SQL Warehouse.
Runs a trivial query directly via databricks-sql-connector.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def ping_databricks():
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "")
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_catalog(), current_user(), current_timestamp()")
            row = cur.fetchone()
            print(f"Connected. catalog={row[0]}, user={row[1]}, ts={row[2]}")
            assert row[0] is not None
            assert row[1] is not None


with DAG(
    dag_id="smoke_test_databricks",
    description="Verify Airflow can reach Databricks SQL Warehouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["day12", "smoke-test"],
) as dag:

    ping = PythonOperator(
        task_id="ping_databricks",
        python_callable=ping_databricks,
    )