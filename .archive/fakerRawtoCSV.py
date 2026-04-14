import os
import csv
import random
from faker import Faker
from datetime import datetime

# Configuration
OUTPUT_DIR = "data/raw"  # Output directory for CSV files
fake = Faker()

def write_csv(filename, headers, data):
    """Helper function to write a list of tuples to a CSV file."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

def generate_telecom_data():
    try:
        # Ensure the output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # 1. Populate Plans (Dimension)
        # Note: Added explicit plan_ids (1-4) to replace the database SERIAL behavior
        plans = [
            (1, 'Basic Saver', 20.00, 5, 500),
            (2, 'Standard Connect', 45.00, 20, 1000),
            (3, 'Unlimited Pro', 80.00, 100, -1), # -1 for unlimited
            (4, 'Family Share', 120.00, 200, 5000)
        ]
        plan_ids = [p[0] for p in plans]
        write_csv("plans.csv", ["plan_id", "plan_name", "monthly_fee", "data_gb", "call_minutes"], plans)

        # 2. Populate Customers (Dimension)
        num_customers = 2000
        customer_data = []
        regions = ['North', 'South', 'East', 'West', 'Central']
        
        for i in range(1, num_customers + 1):
            signup = fake.date_between(start_date='-3y', end_date='-1y')
            churn = fake.date_between(start_date=signup, end_date='today') if random.random() < 0.15 else None
            customer_data.append((
                i, # Explicit customer_id
                fake.name(),
                random.choice(plan_ids),
                random.choice(regions),
                signup,
                churn
            ))
        
        customer_ids = [c[0] for c in customer_data]
        write_csv("customers.csv", ["customer_id", "name", "plan_id", "region", "signup_date", "churn_date"], customer_data)

        # 3. Populate CDR (Fact)
        num_cdr = 12000
        cdr_data = []
        call_types = ['Voice', 'SMS', 'Data_Session']

        for i in range(1, num_cdr + 1):
            start_time = fake.date_time_between(start_date='-30d', end_date='now')
            cdr_data.append((
                i, # Explicit record_id
                random.choice(customer_ids),
                random.randint(1000000, 9999999), # Simulated external number
                start_time,
                random.randint(5, 1200),
                random.choice(call_types),
                random.randint(1, 50) # Tower IDs 1-50
            ))

        write_csv("cdr.csv", ["record_id", "caller_id", "receiver_id", "call_start", "duration_sec", "call_type", "tower_id"], cdr_data)

        # 4. Populate Network Events
        num_events = 500
        event_types = ['Congestion', 'Hardware Failure', 'Firmware Update', 'Power Outage']
        severities = ['Low', 'Medium', 'High', 'Critical']
        event_data = [
            (
                i, # Explicit event_id
                random.randint(1, 50), 
                random.choice(event_types), 
                random.choice(severities), 
                fake.date_time_between(start_date='-30d', end_date='now')
            )
            for i in range(1, num_events + 1)
        ]
        write_csv("network_events.csv", ["event_id", "tower_id", "event_type", "severity", "timestamp"], event_data)

        print(f"Success: Generated {num_customers} customers and {num_cdr} CDR records. Saved to {OUTPUT_DIR}/")

    except Exception as e:
        print(f"Error writing to CSV: {e}")

if __name__ == "__main__":
    generate_telecom_data()