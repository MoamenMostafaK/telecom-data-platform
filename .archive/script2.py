import psycopg2
import pandas as pd
import numpy as np
import os

# 1. Setup your paths and credentials
DB_CONFIG = {
    "dbname": "telecomDB",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

OUTPUT_DIR = "./my_reports"
FILE_NAME = "cleaned_data.csv"

# Create the directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

try:
    # 2. Connect to Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    
    # 3. Use Pandas to run a SQL Query joining two tables
    # Note: Using SQL to join is faster than joining in Python!
    sql_query = """
        SELECT r.call_id, r.cost, c.customer_id, c.full_name, c.national_id
        FROM raw.customers c
        JOIN raw.call_detail_records r ON c.customer_id = r.customer_id
    """
    
    # Load data directly into a DataFrame
    df = pd.read_sql_query(sql_query, conn)
    print("Data successfully pulled from Postgres!")

   
    # --- EXPORTING ---

    # 4. Save to the specific directory
    full_path = os.path.join(OUTPUT_DIR, FILE_NAME)
    df.to_csv(full_path, index=False)
    
    print(f"Process complete! File saved at: {full_path}")

except Exception as e:
    print(f"Something went wrong: {e}")

finally:
    # Always close the connection
    if 'conn' in locals():
        conn.close()
        print("Database connection closed.")