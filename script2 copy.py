import psycopg2
import pandas as pd
import numpy as np
import os

# 1. Setup your paths and credentials
DB_CONFIG = {
    "dbname": "postgres",
    "user": "admin",
    "password": "admin123",
    "host": "localhost",
    "port": "5433"
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
        SELECT u.id, u.name, u.email, o.amount, o.status
        FROM users u
        JOIN orders o ON u.id = o.user_id
    """
    
    # Load data directly into a DataFrame
    df = pd.read_sql_query(sql_query, conn)
    print("Data successfully pulled from Postgres!")

    # --- THE CLEANING STEPS ---

    # A. Use Pandas to handle missing values
    # If a name is missing, fill it with "Unknown Customer"
    df['name'] = df['name'].fillna('Unknown Customer')

    # B. Use NumPy for conditional logic (The "Alter" step)
    # If amount > 100, give a 10% discount, otherwise 0
    df['discount'] = np.where(df['amount'] > 100, df['amount'] * 0.1, 0)
    
    # C. Calculate final total
    df['final_price'] = df['amount'] - df['discount']

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