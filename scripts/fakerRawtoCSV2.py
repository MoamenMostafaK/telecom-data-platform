import csv
import random
from faker import Faker

fake = Faker()
RECORDS = 110000  # Over 100k to ensure substantial data

# Reference Lists
customer_ids = [f"CUST-{i:06d}" for i in range(1, 15001)]
plan_ids = ["BASIC_P", "PRE_5G", "unlimited_v2", "DATA_ONLY", "FAMILY_99"]
tower_ids = [f"TWR-{random.randint(100, 999)}" for _ in range(50)]

def write_to_csv(filename, headers, data_gen_func):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(RECORDS):
            writer.writerow(data_gen_func())
    print(f"Created {filename} with {RECORDS} records.")

# 1. Raw CDR (Call Detail Records)
def gen_cdr():
    # Mess: Mixed duration formats (e.g., "5" vs "5m") and inconsistent case
    duration = random.randint(1, 45)
    return [
        fake.uuid4(),
        random.choice(customer_ids),
        f"+1-{fake.msisdn()[3:]}",
        str(duration) if random.random() > 0.1 else f"{duration} min",
        fake.date_time_between(start_date='-30d', end_date='now'),
        random.choice(["SUCCESS", "dropped", "FAILED", None])
    ]

# 2. Raw Data Usage
def gen_data_usage():
    # Mess: Scientific notation for large numbers or missing tower IDs
    usage = round(random.uniform(0.1, 8000.0), 2)
    return [
        fake.bothify(text='SESS-####-????'),
        random.choice(customer_ids),
        str(usage) if random.random() > 0.05 else f"{usage/1000:.2e}",
        fake.date_time_between(start_date='-30d', end_date='now'),
        random.choice(tower_ids) if random.random() > 0.1 else "UNKNOWN"
    ]

# 3. Raw Recharges
def gen_recharges():
    # Mess: Inconsistent currency symbols and messy dates
    return [
        f"REC-{random.randint(100000, 999999)}",
        random.choice(customer_ids),
        f"${random.randint(10, 100)}" if random.random() > 0.2 else random.randint(10, 100),
        fake.date_this_year(),
        random.choice(["VISA", "CASH", "App_Wallet", "null"])
    ]

# Execute generation
write_to_csv('raw_cdr.csv', ['call_id', 'caller_id', 'receiver_id', 'duration', 'timestamp', 'status'], gen_cdr)
write_to_csv('raw_data_usage.csv', ['session_id', 'customer_id', 'mb_used', 'start_time', 'tower_id'], gen_data_usage)
write_to_csv('raw_recharges.csv', ['recharge_id', 'customer_id', 'amount', 'date', 'method'], gen_recharges)
