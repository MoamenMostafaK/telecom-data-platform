# src/transformation/transform.py
from src.ingestion.db_client import PostgresClient

def deduplicate_customers():
    client = PostgresClient()

    dedup_query = """
    CREATE TABLE IF NOT EXISTS staging.customers AS
    SELECT DISTINCT ON (customer_id) *
    FROM raw.customers
    ORDER BY customer_id, ingestion_timestamp DESC;
    """
    client.execute(dedup_query)
    client.close()
