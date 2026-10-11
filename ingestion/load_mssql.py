"""
Load Olist CSVs from data/raw/ into MSSQL olist database.

Run inside the Airflow container:
    python /opt/airflow/ingestion/load_mssql.py
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv


# ----- Config -----
RAW_DIR = Path("/opt/airflow/data/raw") if Path("/opt/airflow").exists() else Path(__file__).parent.parent / "data" / "raw"
MSSQL_SERVER = "mssql"
MSSQL_PORT = 1433
MSSQL_USER = "sa"
MSSQL_DATABASE = "olist"
CHUNK_SIZE = 50_000


# Explicit column sizes for setinputsizes (only used for tables that need it).
#   None      -> pyodbc infers
#   int       -> string buffer size
#   (p, s)    -> numeric precision/scale
COLUMN_SIZES = {
    "customers": {
        "customer_id": 100,
        "customer_unique_id": 100,
        "customer_zip_code_prefix": 100,
        "customer_city": 500,
        "customer_state": 10,
    },
    "geolocation": {
        "geolocation_zip_code_prefix": 100,
        "geolocation_lat": None,
        "geolocation_lng": None,
        "geolocation_city": 500,
        "geolocation_state": 10,
    },
}


# Only these tables get explicit setinputsizes hints.
# Others rely on pyodbc auto-sizing (avoids INT/DECIMAL binding quirks).
TABLES_NEEDING_SIZES = {"customers", "geolocation"}


# table_name -> csv_filename
TABLES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

# Columns parsed as datetime
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

# Columns parsed as numeric (float)
NUMERIC_COLUMNS = {
    "geolocation": ["geolocation_lat", "geolocation_lng"],
    "order_items": ["order_item_id", "price", "freight_value"],
    "order_payments": ["payment_sequential", "payment_installments", "payment_value"],
    "order_reviews": ["review_score"],
    "products": [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
}


# ----- Helpers -----
def get_password() -> str:
    load_dotenv(Path(__file__).parent.parent / ".env")
    pw = os.environ.get("MSSQL_SA_PASSWORD")
    if not pw:
        sys.exit("ERROR: MSSQL_SA_PASSWORD not set")
    return pw


def get_connection(password: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={MSSQL_SERVER},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DATABASE};"
        f"UID={MSSQL_USER};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def load_table(conn: pyodbc.Connection, table: str, csv_path: Path) -> int:
    print(f"\n-> Loading {table} from {csv_path.name} ...")

    t0 = time.time()
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # Parse date columns
    for col in DATE_COLUMNS.get(table, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Parse numeric columns
    for col in NUMERIC_COLUMNS.get(table, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where numeric columns could not be parsed
    # (Olist has a few malformed rows -- safe to drop them)
    numeric_cols_present = [c for c in NUMERIC_COLUMNS.get(table, []) if c in df.columns]
    if numeric_cols_present:
        before = len(df)
        df = df.dropna(subset=numeric_cols_present)
        dropped = before - len(df)
        if dropped:
            print(f"  Dropped {dropped} rows with invalid numeric values")

    # Replace empty strings with None
    df = df.replace({"": None})

    # Convert NaN -> None (pyodbc can't bind NaN to SQL float/int)
    df = df.where(pd.notna(df), None)

    print(f"  Read {len(df):,} rows in {time.time() - t0:.1f}s")

    if len(df) == 0:
        print("  (empty file - skipping insert)")
        return 0

    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(f"[{c}]" for c in cols)
    sql = f"INSERT INTO dbo.{table} ({col_list}) VALUES ({placeholders})"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # Only use setinputsizes for tables that need it
    if table in TABLES_NEEDING_SIZES:
        table_sizes = COLUMN_SIZES.get(table, {})
        sizes = [table_sizes.get(c, None) for c in cols]
    else:
        sizes = None

    # Chunked bulk insert
    t0 = time.time()
    inserted = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i : i + CHUNK_SIZE]
        cur = conn.cursor()
        cur.fast_executemany = True
        try:
            if sizes is not None:
                cur.setinputsizes(sizes)
            cur.executemany(sql, chunk)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  ERROR at chunk {i}-{i + len(chunk)}: {e}")
            raise
        finally:
            cur.close()
        inserted += len(chunk)
        print(f"    {inserted:,}/{len(rows):,} rows inserted")

    print(f"  Inserted {len(rows):,} rows in {time.time() - t0:.1f}s")
    return len(rows)


def main():
    print("=" * 60)
    print("Olist CSV -> MSSQL loader")
    print("=" * 60)

    if not RAW_DIR.exists():
        sys.exit(f"ERROR: {RAW_DIR} does not exist")

    password = get_password()
    print(f"\nConnecting to MSSQL at {MSSQL_SERVER}:{MSSQL_PORT} ...")
    conn = get_connection(password)
    print("  Connected.")

    total_rows = 0
    for table, csv_file in TABLES.items():
        csv_path = RAW_DIR / csv_file
        if not csv_path.exists():
            print(f"\nSkipping {table}: {csv_path.name} not found")
            continue
        rows = load_table(conn, table, csv_path)
        total_rows += rows

    conn.close()
    print("\n" + "=" * 60)
    print(f"Done. Total rows loaded: {total_rows:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()