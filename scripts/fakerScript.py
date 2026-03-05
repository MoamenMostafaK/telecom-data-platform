import os
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
from datetime import datetime, timedelta


# Configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://admin:admin@localhost:5498/telecomDB")

fake = Faker()

def generate_telecom_data():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # 1. Schema Setup
        cur.execute("""
            DROP TABLE IF EXISTS cdr CASCADE;
            DROP TABLE IF EXISTS network_events CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
            DROP TABLE IF EXISTS plans CASCADE;

            CREATE TABLE plans (
                plan_id SERIAL PRIMARY KEY,
                plan_name TEXT,
                monthly_fee NUMERIC(10, 2),
                data_gb INTEGER,
                call_minutes INTEGER
            );

            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                name TEXT,
                plan_id INTEGER REFERENCES plans(plan_id),
                region TEXT,
                signup_date DATE,
                churn_date DATE
            );

            CREATE TABLE cdr (
                record_id SERIAL PRIMARY KEY,
                caller_id INTEGER REFERENCES customers(customer_id),
                receiver_id INTEGER,
                call_start TIMESTAMP,
                duration_sec INTEGER,
                call_type TEXT,
                tower_id INTEGER
            );

            CREATE TABLE network_events (
                event_id SERIAL PRIMARY KEY,
                tower_id INTEGER,
                event_type TEXT,
                severity TEXT,
                timestamp TIMESTAMP
            );
        """)
        # 2. Populate Plans (Dimension)
        plans = [
            ('Basic Saver', 20.00, 5, 500),
            ('Standard Connect', 45.00, 20, 1000),
            ('Unlimited Pro', 80.00, 100, -1), # -1 for unlimited
            ('Family Share', 120.00, 200, 5000)
        ]
        execute_values(cur, "INSERT INTO plans (plan_name, monthly_fee, data_gb, call_minutes) VALUES %s", plans)
        
        # Get plan IDs for foreign keys
        cur.execute("SELECT plan_id FROM plans")
        plan_ids = [r[0] for r in cur.fetchall()]

        # 3. Populate Customers (Dimension)
        num_customers = 2000
        customer_data = []
        regions = ['North', 'South', 'East', 'West', 'Central']
        
        for _ in range(num_customers):
            signup = fake.date_between(start_date='-3y', end_date='-1y')
            churn = fake.date_between(start_date=signup, end_date='today') if random.random() < 0.15 else None
            customer_data.append((
                fake.name(),
                random.choice(plan_ids),
                random.choice(regions),
                signup,
                churn
            ))
        
        execute_values(cur, "INSERT INTO customers (name, plan_id, region, signup_date, churn_date) VALUES %s", customer_data)
        
        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [r[0] for r in cur.fetchall()]

        # 4. Populate CDR (Fact) - 10,000+ rows
        num_cdr = 12000
        cdr_data = []
        call_types = ['Voice', 'SMS', 'Data_Session']

        for _ in range(num_cdr):
            start_time = fake.date_time_between(start_date='-30d', end_date='now')
            cdr_data.append((
                random.choice(customer_ids),
                random.randint(1000000, 9999999), # Simulated external number
                start_time,
                random.randint(5, 1200),
                random.choice(call_types),
                random.randint(1, 50) # Tower IDs 1-50
            ))

        execute_values(cur, "INSERT INTO cdr (caller_id, receiver_id, call_start, duration_sec, call_type, tower_id) VALUES %s", cdr_data)

        # 5. Populate Network Events (Optional)
        num_events = 500
        event_types = ['Congestion', 'Hardware Failure', 'Firmware Update', 'Power Outage']
        severities = ['Low', 'Medium', 'High', 'Critical']
        event_data = [
            (random.randint(1, 50), random.choice(event_types), random.choice(severities), fake.date_time_between(start_date='-30d', end_date='now'))
            for _ in range(num_events)
        ]
        execute_values(cur, "INSERT INTO network_events (tower_id, event_type, severity, timestamp) VALUES %s", event_data)

        conn.commit()
        print(f"Success: Generated {num_customers} customers and {num_cdr} CDR records.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    generate_telecom_data()