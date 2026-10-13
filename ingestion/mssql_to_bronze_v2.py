"""
Fast MSSQL -> Databricks bronze ELT using Parquet + COPY INTO.

Strategy:
1. Export each MSSQL table to a local Parquet file (fast, columnar)
2. Upload Parquet files to a Databricks Unity Catalog Volume
3. Run COPY INTO from the Volume into the bronze table

Why this is faster than INSERT:
- Parquet is columnar + compressed (~30 MB total for 1.33M rows)
- COPY INTO is Databricks-native, runs distributed
- One statement per table instead of thousands
"""
import os
import sys
import time
import warnings
from pathlib import Path

import pandas as pd
import pyodbc
from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Suppress pandas SQLAlchemy warning (we intentionally use pyodbc)
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


# ----- Config -----
PROJECT_ROOT = Path(__file__).parent.parent

MSSQL_SERVER = "mssql"
MSSQL_PORT = 1433
MSSQL_USER = "sa"
MSSQL_DATABASE = "olist"

DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "olist")
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "staging_files"

# Local parquet staging directory inside the container
LOCAL_PARQUET_DIR = Path("/opt/airflow/data/parquet")

# Volume path inside Databricks
VOLUME_PATH = f"/Volumes/{DATABRICKS_CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"


TABLES = [
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
]

# Datetime columns for pandas parsing
DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
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


def get_workspace_client(host: str, token: str) -> WorkspaceClient:
    """SDK client for uploading files to Unity Catalog Volumes."""
    return WorkspaceClient(host=f"https://{host}", token=token)


def export_table_to_parquet(mssql_conn, table_name: str) -> Path:
    """Read a MSSQL table into a Pandas DataFrame, write to Parquet."""
    print(f"\n-> Exporting {table_name} to Parquet ...")

    t0 = time.time()

    # Read full table
    sql = f"SELECT * FROM dbo.{table_name}"
    df = pd.read_sql(sql, mssql_conn)

    # Cast datetime columns to microsecond precision (Databricks requires this)
    for col in DATE_COLUMNS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")

    print(f"  Read {len(df):,} rows in {time.time() - t0:.1f}s")

    # Ensure the local staging dir exists
    LOCAL_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Write to Parquet
    parquet_path = LOCAL_PARQUET_DIR / f"{table_name}.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", index=False, compression="snappy")

    file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  Wrote {parquet_path.name} ({file_size_mb:.1f} MB) in {time.time() - t0:.1f}s")

    return parquet_path


def upload_parquet_to_volume(client: WorkspaceClient, local_path: Path, table_name: str):
    """Upload a local Parquet file to the Unity Catalog Volume."""
    volume_file_path = f"{VOLUME_PATH}/{table_name}.parquet"

    print(f"  Uploading to {volume_file_path} ...")
    t0 = time.time()

    with open(local_path, "rb") as f:
        client.files.upload(file_path=volume_file_path, contents=f, overwrite=True)

    print(f"  Uploaded in {time.time() - t0:.1f}s")


def copy_into_bronze(db_conn, table_name: str):
    """Load the Parquet file into the bronze table via CREATE TABLE AS SELECT."""
    full_table = f"{DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    source_path = f"{VOLUME_PATH}/{table_name}.parquet"

    print(f"  Loading into {full_table} ...")
    t0 = time.time()

    # Drop and recreate the table to ensure clean state
    with db_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {full_table}")

    # Create table from Parquet (schema inferred from file)
    create_sql = f"""
    CREATE TABLE {full_table}
    USING DELTA
    AS SELECT * FROM parquet.`{source_path}`
    """

    with db_conn.cursor() as cur:
        cur.execute(create_sql)

    # Row count check
    with db_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {full_table}")
        count = cur.fetchone()[0]

    print(f"  Loaded {count:,} rows in {time.time() - t0:.1f}s")
    return count


def get_mssql_row_count(mssql_conn, table_name: str) -> int:
    with mssql_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        return cur.fetchone()[0]


# ----- Main -----
def main():
    print("=" * 60)
    print("MSSQL -> Databricks bronze (Parquet + COPY INTO)")
    print("=" * 60)

    # Connect to MSSQL
    mssql_pw = get_mssql_password()
    print(f"\nConnecting to MSSQL ...")
    mssql_conn = get_mssql_connection(mssql_pw)
    print("  Connected.")

    # Connect to Databricks
    host, http_path, token = get_databricks_config()
    print(f"\nConnecting to Databricks ...")
    db_conn = get_databricks_connection(host, http_path, token)
    db_client = get_workspace_client(host, token)
    print("  Connected.")

    results = []
    total_start = time.time()

    for table_name in TABLES:
        print(f"\n{'=' * 60}")
        print(f"Processing: {table_name}")
        print(f"{'=' * 60}")

        mssql_rows = get_mssql_row_count(mssql_conn, table_name)
        print(f"  MSSQL rows: {mssql_rows:,}")

        # 1. Export to Parquet
        parquet_path = export_table_to_parquet(mssql_conn, table_name)

        # 2. Upload to Volume
        upload_parquet_to_volume(db_client, parquet_path, table_name)

        # 3. Load into bronze
        databricks_rows = copy_into_bronze(db_conn, table_name)

        status = "OK" if mssql_rows == databricks_rows else "MISMATCH"
        results.append({
            "table": table_name,
            "mssql": mssql_rows,
            "databricks": databricks_rows,
            "status": status,
        })

    # Summary
    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"{'Table':<40} {'MSSQL':>10} {'Databricks':>12}  Status")
    print("-" * 70)
    total_src = 0
    total_dst = 0
    for r in results:
        print(f"{r['table']:<40} {r['mssql']:>10,} {r['databricks']:>12,}  {r['status']}")
        total_src += r["mssql"]
        total_dst += r["databricks"]
    print("-" * 70)
    print(f"{'TOTAL':<40} {total_src:>10,} {total_dst:>12,}")
    print(f"\nTotal time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")

    mssql_conn.close()
    db_conn.close()

    print("\n" + "=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()