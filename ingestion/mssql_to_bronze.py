"""
Extract all tables from MSSQL olist database and land them into
Databricks bronze schema as Delta tables.

Run inside the Airflow container:
    python /opt/airflow/ingestion/mssql_to_bronze.py
"""
import os
import sys
import time
from pathlib import Path

import pyodbc
from databricks import sql as databricks_sql
from dotenv import load_dotenv


# ----- Config -----
PROJECT_ROOT = Path(__file__).parent.parent

MSSQL_SERVER = "mssql"
MSSQL_PORT = 1433
MSSQL_USER = "sa"
MSSQL_DATABASE = "olist"

DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "olist")
BRONZE_SCHEMA = "bronze"

READ_CHUNK_SIZE = 50_000
WRITE_BATCH_SIZE = 5_000  # bumped from 500 to reduce round-trips 10x


# Column definitions for Databricks bronze tables.
TABLE_SCHEMAS = {
    "customers": [
        ("customer_id", "STRING"),
        ("customer_unique_id", "STRING"),
        ("customer_zip_code_prefix", "STRING"),
        ("customer_city", "STRING"),
        ("customer_state", "STRING"),
    ],
    "geolocation": [
        ("geolocation_zip_code_prefix", "STRING"),
        ("geolocation_lat", "DOUBLE"),
        ("geolocation_lng", "DOUBLE"),
        ("geolocation_city", "STRING"),
        ("geolocation_state", "STRING"),
    ],
    "orders": [
        ("order_id", "STRING"),
        ("customer_id", "STRING"),
        ("order_status", "STRING"),
        ("order_purchase_timestamp", "TIMESTAMP"),
        ("order_approved_at", "TIMESTAMP"),
        ("order_delivered_carrier_date", "TIMESTAMP"),
        ("order_delivered_customer_date", "TIMESTAMP"),
        ("order_estimated_delivery_date", "TIMESTAMP"),
    ],
    "order_items": [
        ("order_id", "STRING"),
        ("order_item_id", "INT"),
        ("product_id", "STRING"),
        ("seller_id", "STRING"),
        ("shipping_limit_date", "TIMESTAMP"),
        ("price", "DECIMAL(18, 2)"),
        ("freight_value", "DECIMAL(18, 2)"),
    ],
    "order_payments": [
        ("order_id", "STRING"),
        ("payment_sequential", "INT"),
        ("payment_type", "STRING"),
        ("payment_installments", "INT"),
        ("payment_value", "DECIMAL(18, 2)"),
    ],
    "order_reviews": [
        ("review_id", "STRING"),
        ("order_id", "STRING"),
        ("review_score", "INT"),
        ("review_comment_title", "STRING"),
        ("review_comment_message", "STRING"),
        ("review_creation_date", "TIMESTAMP"),
        ("review_answer_timestamp", "TIMESTAMP"),
    ],
    "products": [
        ("product_id", "STRING"),
        ("product_category_name", "STRING"),
        ("product_name_lenght", "INT"),
        ("product_description_lenght", "INT"),
        ("product_photos_qty", "INT"),
        ("product_weight_g", "INT"),
        ("product_length_cm", "INT"),
        ("product_height_cm", "INT"),
        ("product_width_cm", "INT"),
    ],
    "sellers": [
        ("seller_id", "STRING"),
        ("seller_zip_code_prefix", "STRING"),
        ("seller_city", "STRING"),
        ("seller_state", "STRING"),
    ],
    "product_category_name_translation": [
        ("product_category_name", "STRING"),
        ("product_category_name_english", "STRING"),
    ],
}


# ----- Helpers -----
def get_mssql_password() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    pw = os.environ.get("MSSQL_SA_PASSWORD")
    if not pw:
        sys.exit("ERROR: MSSQL_SA_PASSWORD not set")
    return pw


def get_databricks_config():
    load_dotenv(PROJECT_ROOT / ".env")
    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not all([host, http_path, token]):
        sys.exit("ERROR: Missing DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN")
    return host, http_path, token


def get_mssql_connection(password: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={MSSQL_SERVER},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DATABASE};"
        f"UID={MSSQL_USER};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def get_databricks_connection(host: str, http_path: str, token: str):
    return databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def drop_and_create_databricks_table(db_conn, table_name: str):
    full_name = f"{DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    cols = TABLE_SCHEMAS[table_name]
    col_defs = ",\n    ".join(f"{col} {dtype}" for col, dtype in cols)

    with db_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {full_name}")
        cur.execute(f"CREATE TABLE {full_name} (\n    {col_defs}\n) USING DELTA")


def fetch_chunk(mssql_conn, table_name: str, offset: int, limit: int):
    cols = [col for col, _ in TABLE_SCHEMAS[table_name]]
    col_list = ", ".join(f"[{c}]" for c in cols)

    sql = f"""
    SELECT {col_list}
    FROM dbo.{table_name}
    ORDER BY (SELECT NULL)
    OFFSET {offset} ROWS
    FETCH NEXT {limit} ROWS ONLY
    """
    cur = mssql_conn.cursor()
    cur.execute(sql)
    return cur.fetchall(), cur


def insert_batch_to_databricks(db_conn, table_name: str, rows):
    """Insert a batch of rows into Databricks using multi-row INSERT."""
    if not rows:
        return

    full_name = f"{DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    cols = [col for col, _ in TABLE_SCHEMAS[table_name]]
    col_list = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))

    values_clause = ", ".join([f"({placeholders})"] * len(rows))
    sql = f"INSERT INTO {full_name} ({col_list}) VALUES {values_clause}"

    flat_params = [val for row in rows for val in row]

    with db_conn.cursor() as cur:
        cur.execute(sql, flat_params)


def get_mssql_row_count(mssql_conn, table_name: str) -> int:
    with mssql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        return cur.fetchone()[0]


def migrate_table(mssql_conn, db_conn, table_name: str) -> dict:
    print(f"\n-> Migrating {table_name} ...")

    t0 = time.time()
    total_rows = get_mssql_row_count(mssql_conn, table_name)
    print(f"  MSSQL row count: {total_rows:,}")

    if total_rows == 0:
        print("  (empty table - dropping + recreating only)")
        drop_and_create_databricks_table(db_conn, table_name)
        return {"table": table_name, "source_rows": 0, "dest_rows": 0, "elapsed": 0}

    print(f"  Dropping + creating {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{table_name} ...")
    drop_and_create_databricks_table(db_conn, table_name)

    offset = 0
    inserted = 0
    while offset < total_rows:
        chunk_rows, cur = fetch_chunk(mssql_conn, table_name, offset, READ_CHUNK_SIZE)
        cur.close()

        # Sub-batch the chunk for Databricks INSERT
        for i in range(0, len(chunk_rows), WRITE_BATCH_SIZE):
            sub_batch = chunk_rows[i : i + WRITE_BATCH_SIZE]
            insert_batch_to_databricks(db_conn, table_name, sub_batch)
            inserted += len(sub_batch)

        offset += len(chunk_rows)
        print(f"    {inserted:,}/{total_rows:,} rows transferred")

    elapsed = time.time() - t0
    print(f"  Done: {inserted:,} rows in {elapsed:.1f}s")

    return {
        "table": table_name,
        "source_rows": total_rows,
        "dest_rows": inserted,
        "elapsed": elapsed,
    }


# ----- Main -----
def main():
    print("=" * 60)
    print("MSSQL -> Databricks bronze ELT")
    print("=" * 60)

    mssql_pw = get_mssql_password()
    print(f"\nConnecting to MSSQL at {MSSQL_SERVER}:{MSSQL_PORT} ...")
    mssql_conn = get_mssql_connection(mssql_pw)
    print("  Connected.")

    host, http_path, token = get_databricks_config()
    print(f"\nConnecting to Databricks at {host} ...")
    db_conn = get_databricks_connection(host, http_path, token)
    print("  Connected.")

    results = []
    for table_name in TABLE_SCHEMAS.keys():
        result = migrate_table(mssql_conn, db_conn, table_name)
        results.append(result)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"{'Table':<40} {'Source':>10} {'Dest':>10} {'Time':>8}")
    print("-" * 70)
    total_source = 0
    total_dest = 0
    total_time = 0.0
    for r in results:
        status = "OK" if r["source_rows"] == r["dest_rows"] else "MISMATCH"
        print(f"{r['table']:<40} {r['source_rows']:>10,} {r['dest_rows']:>10,} {r['elapsed']:>7.1f}s {status}")
        total_source += r["source_rows"]
        total_dest += r["dest_rows"]
        total_time += r["elapsed"]
    print("-" * 70)
    print(f"{'TOTAL':<40} {total_source:>10,} {total_dest:>10,} {total_time:>7.1f}s")

    mssql_conn.close()
    db_conn.close()

    print("\n" + "=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()