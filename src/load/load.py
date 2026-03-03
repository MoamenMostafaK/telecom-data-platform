# src/load/load.py
from src.ingestion.db_client import PostgresClient

def create_fact_calls():
    client = PostgresClient()

    fact_query = """
    CREATE TABLE IF NOT EXISTS staging.fact_calls AS
    SELECT
        cdr.call_id,
        cdr.customer_id,
        cdr.call_type,
        cdr.duration_seconds,
        cdr.cost
    FROM raw.call_detail_records cdr
    JOIN staging.customers c
        ON cdr.customer_id = c.customer_id;
    """
    client.execute(fact_query)
    client.close()
