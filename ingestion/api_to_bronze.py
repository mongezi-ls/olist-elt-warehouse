"""
Fetch exchange rates from ExchangeRate-API and land raw data into
Databricks bronze schema.

Run inside the Airflow container:
    python /opt/airflow/ingestion/api_to_bronze.py
"""
import os
import sys
import time
from datetime import datetime, timezone

import requests
from databricks import sql as databricks_sql
from dotenv import load_dotenv


# ----- Config -----
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "olist")
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "exchange_rates_raw"

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
REQUEST_TIMEOUT = 30


# ----- Helpers -----
def get_databricks_config():
    """Load Databricks connection config from env / .env."""
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not all([host, http_path, token]):
        sys.exit("ERROR: Missing DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN")
    return host, http_path, token


def fetch_rates_with_retry(url: str) -> dict:
    """Fetch rates from API with exponential backoff retries."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES} ...")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"    Failed: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                print(f"    Waiting {wait}s before retry ...")
                time.sleep(wait)
    raise RuntimeError(f"API request failed after {MAX_RETRIES} attempts: {last_error}")


def get_databricks_connection(host: str, http_path: str, token: str):
    """Open a Databricks SQL connection."""
    return databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def ensure_bronze_table(conn):
    """Create bronze table if it doesn't exist."""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE} (
        base_currency   STRING,
        rate_date       DATE,
        target_currency STRING,
        rate            DOUBLE,
        ingested_at     TIMESTAMP
    )
    USING DELTA
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    print(f"  Ensured table {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")


def insert_rates(conn, rates_payload: dict):
    """Insert each currency pair as a row into the bronze table."""
    base_currency = rates_payload["base"]
    rate_date = rates_payload["date"]
    rates = rates_payload["rates"]
    ingested_at = datetime.now(timezone.utc)

    rows = [
        (base_currency, rate_date, target_currency, float(rate), ingested_at)
        for target_currency, rate in rates.items()
    ]

    print(f"  Inserting {len(rows)} currency rates ...")

    insert_sql = f"""
    INSERT INTO {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}
    (base_currency, rate_date, target_currency, rate, ingested_at)
    VALUES (?, ?, ?, ?, ?)
    """

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)
    print(f"  Inserted {len(rows)} rows")


def verify_ingestion(conn):
    """Query the table to confirm data landed."""
    query = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT target_currency) AS distinct_currencies,
        MAX(rate_date) AS latest_date,
        MAX(ingested_at) AS latest_ingestion
    FROM {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        print(f"\n  Verification:")
        print(f"    Total rows:           {row[0]:,}")
        print(f"    Distinct currencies:  {row[1]}")
        print(f"    Latest rate date:     {row[2]}")
        print(f"    Latest ingestion:     {row[3]}")


# ----- Main -----
def main():
    print("=" * 60)
    print("API -> Databricks bronze ingestion")
    print("=" * 60)

    print(f"\n[1/4] Fetching rates from {API_URL} ...")
    payload = fetch_rates_with_retry(API_URL)
    print(f"  Got rates for {len(payload['rates'])} currencies (base: {payload['base']})")

    print(f"\n[2/4] Connecting to Databricks ...")
    host, http_path, token = get_databricks_config()
    conn = get_databricks_connection(host, http_path, token)
    print(f"  Connected to {host}")

    print(f"\n[3/4] Ensuring bronze table exists ...")
    ensure_bronze_table(conn)

    print(f"\n[4/4] Inserting rates ...")
    insert_rates(conn, payload)

    verify_ingestion(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()