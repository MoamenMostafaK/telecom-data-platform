import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
np.random.seed(42)

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / 'generate' / 'data' / 'raw'
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Egyptian context ──────────────────────────────────────────────────────────
EGYPTIAN_FIRST_NAMES = [
    'Moamen', 'Ahmed', 'Mahmoud', 'Omar', 'Ali', 'Hassan', 'Hussein',
    'Ibrahim', 'Khaled', 'Tarek', 'Youssef', 'Amr', 'Mostafa', 'Karim',
    'Fatma', 'Sara', 'Mariam', 'Aya', 'Dina', 'Rania', 'Mona','Heba',
    'Yasmin', 'Salma', 'Noha', 'Eman', 'Hana', 'Asmaa', 'Doaa', 'Mohamed'
]

EGYPTIAN_LAST_NAMES = [
    'Mohamed', 'Ahmed', 'Hassan', 'Ibrahim', 'Mostafa', 'Ali', 'Omar',
    'Abdallah', 'Abdelrahman', 'Abdelaziz', 'Soliman', 'Sayed', 'Mahmoud',
    'Khalil', 'Mansour', 'Farouk', 'Nasser', 'Saad', 'Ragab', 'Younis'
]

CITIES_REGIONS = [
    ('Cairo', 'Cairo'),
    ('Giza', 'Giza'),
    ('Alexandria', 'Alexandria'),
    ('Aswan', 'Upper Egypt'),
    ('Luxor', 'Upper Egypt'),
    ('Asyut', 'Upper Egypt'),
    ('Sohag', 'Upper Egypt'),
    ('Minya', 'Upper Egypt'),
    ('Qena', 'Upper Egypt'),
    ('Port Said', 'Canal Zone'),
    ('Ismailia', 'Canal Zone'),
    ('Suez', 'Canal Zone'),
    ('Mansoura', 'Delta'),
    ('Tanta', 'Delta'),
    ('Zagazig', 'Delta'),
    ('Damietta', 'Delta'),
    ('Sharm El Sheikh', 'South Sinai'),
    ('Hurghada', 'Red Sea'),
    ('Marsa Matruh', 'Northwest Coast'),
    ('Beni Suef', 'Middle Egypt'),
]

MOBILE_PREFIXES = ['010', '011', '012', '015']  # Vodafone, Etisalat/e&, Orange, WE

TOWER_VENDORS = ['Huawei', 'Ericsson', 'Nokia']
TOWER_TECHNOLOGIES = ['2G', '3G', '4G', '5G']
RECHARGE_CHANNELS = ['app', 'ussd', 'retailer', 'online']
CDR_TYPES = ['voice', 'sms', 'data']
CHURN_REASONS = ['voluntary', 'involuntary', 'ported']
CUSTOMER_STATUSES = ['active', 'suspended', 'churned']


def generate_msisdn(valid=True):
    prefix = random.choice(MOBILE_PREFIXES)
    if valid:
        number = ''.join([str(random.randint(0, 9)) for _ in range(8)])
        return f'+2{prefix}{number}'
    else:
        # Invalid: wrong digit count
        number = ''.join([str(random.randint(0, 9)) for _ in range(random.choice([5, 6, 10]))])
        return f'+2{prefix}{number}'


def generate_national_id():
    # Egyptian national ID: 14 digits
    return ''.join([str(random.randint(0, 9)) for _ in range(14)])


# ── 1. CUSTOMERS ─────────────────────────────────────────────────────────────
def generate_customers(n=1000):
    print(f'Generating {n} customers...')
    records = []
    msisdns = set()

    for i in range(n):
        city, region = random.choice(CITIES_REGIONS)
        msisdn = generate_msisdn(valid=True)
        while msisdn in msisdns:
            msisdn = generate_msisdn(valid=True)
        msisdns.add(msisdn)

        reg_date = datetime(2018, 1, 1) + timedelta(days=random.randint(0, 365 * 5))

        record = {
            'customer_id': str(uuid.uuid4()),
            'msisdn': msisdn,
            'full_name': f"{random.choice(EGYPTIAN_FIRST_NAMES)} {random.choice(EGYPTIAN_LAST_NAMES)}",
            'city': city,
            'region': region,
            'national_id': generate_national_id(),
            'registration_date': reg_date.strftime('%Y-%m-%d'),
            'status': random.choices(CUSTOMER_STATUSES, weights=[0.75, 0.15, 0.10])[0],
            'balance_egp': round(random.uniform(0, 500), 2),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # ── Inject dirty data ──
    # 5% missing city/region
    mask = np.random.random(len(df)) < 0.05
    df.loc[mask, 'city'] = None
    df.loc[mask, 'region'] = None

    # 3% invalid MSISDN
    invalid_idx = df.sample(frac=0.03).index
    df.loc[invalid_idx, 'msisdn'] = [generate_msisdn(valid=False) for _ in range(len(invalid_idx))]

    # 2% duplicate rows
    dupes = df.sample(frac=0.02)
    df = pd.concat([df, dupes], ignore_index=True)

    df['msisdn'] = df['msisdn'].astype(str)
    df.to_csv(RAW_DIR / 'customers.csv', index=False)
    print(f'  → {len(df)} rows written (including dupes and dirty)')
    return list(msisdns)


# ── 2. CELL TOWERS ───────────────────────────────────────────────────────────
def generate_cell_towers(n=50):
    print(f'Generating {n} cell towers...')
    records = []

    for _ in range(n):
        city, region = random.choice(CITIES_REGIONS)
        record = {
            'tower_id': str(uuid.uuid4()),
            'city': city,
            'region': region,
            'latitude': round(random.uniform(22.0, 31.5), 6),   # Egypt lat range
            'longitude': round(random.uniform(25.0, 37.0), 6),  # Egypt lon range
            'technology': random.choice(TOWER_TECHNOLOGIES),
            'vendor': random.choice(TOWER_VENDORS),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df.to_csv(RAW_DIR / 'cell_towers.csv', index=False)
    print(f'  → {len(df)} rows written')
    return df['tower_id'].tolist()


# ── 3. CDR ───────────────────────────────────────────────────────────────────
def generate_cdr(msisdns, tower_ids, n=50000):
    print(f'Generating {n} CDR records...')
    records = []

    for _ in range(n):
        record_type = random.choices(CDR_TYPES, weights=[0.5, 0.3, 0.2])[0]
        start_ts = datetime(2021, 1, 1) + timedelta(
            days=random.randint(0, 730),
            seconds=random.randint(0, 86400)
        )

        if record_type == 'voice':
            duration = random.randint(10, 3600)
            data_mb = None
            cost = round(duration / 60 * random.uniform(0.15, 0.45), 2)
            end_ts = start_ts + timedelta(seconds=duration)
        elif record_type == 'sms':
            duration = None
            data_mb = None
            cost = round(random.uniform(0.05, 0.25), 2)
            end_ts = start_ts + timedelta(seconds=random.randint(1, 5))
        else:  # data
            duration = None
            data_mb = round(random.uniform(0.1, 500), 2)
            cost = round(data_mb * random.uniform(0.01, 0.05), 2)
            end_ts = start_ts + timedelta(seconds=random.randint(30, 7200))

        record = {
            'cdr_id': str(uuid.uuid4()),
            'msisdn': random.choice(msisdns),
            'called_msisdn': random.choice(msisdns) if record_type in ['voice', 'sms'] else None,
            'tower_id': random.choice(tower_ids),
            'record_type': record_type,
            'duration_seconds': duration,
            'data_mb': data_mb,
            'cost_egp': cost,
            'start_timestamp': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
            'end_timestamp': end_ts.strftime('%Y-%m-%d %H:%M:%S'),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # ── Inject dirty data ──
    # 10% NULL duration on voice (dropped calls)
    voice_mask = (df['record_type'] == 'voice') & (np.random.random(len(df)) < 0.10)
    df.loc[voice_mask, 'duration_seconds'] = None

    # 5% NULL tower_id (dead zones)
    tower_mask = np.random.random(len(df)) < 0.05
    df.loc[tower_mask, 'tower_id'] = None

    # 3% duplicate cdr_id
    dupes = df.sample(frac=0.03).copy()
    df = pd.concat([df, dupes], ignore_index=True)

    # 2% end_timestamp before start_timestamp (system clock errors)
    swap_mask = np.random.random(len(df)) < 0.02
    df.loc[swap_mask, ['start_timestamp', 'end_timestamp']] = \
        df.loc[swap_mask, ['end_timestamp', 'start_timestamp']].values

    df.to_csv(RAW_DIR / 'cdr.csv', index=False)
    print(f'  → {len(df)} rows written (including dupes and dirty)')


# ── 4. RECHARGES ─────────────────────────────────────────────────────────────
def generate_recharges(msisdns, n=10000):
    print(f'Generating {n} recharge records...')
    records = []

    RECHARGE_AMOUNTS = [5, 10, 15, 20, 25, 30, 50, 100, 150, 200]

    for _ in range(n):
        ts = datetime(2021, 1, 1) + timedelta(
            days=random.randint(0, 730),
            seconds=random.randint(0, 86400)
        )
        record = {
            'recharge_id': str(uuid.uuid4()),
            'msisdn': random.choice(msisdns),
            'amount_egp': random.choice(RECHARGE_AMOUNTS),
            'recharge_timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'channel': random.choice(RECHARGE_CHANNELS),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # ── Inject dirty data ──
    # 3% negative amounts (system errors)
    neg_mask = np.random.random(len(df)) < 0.03
    df.loc[neg_mask, 'amount_egp'] = df.loc[neg_mask, 'amount_egp'] * -1

    # 2% NULL channel
    null_mask = np.random.random(len(df)) < 0.02
    df.loc[null_mask, 'channel'] = None

    df.to_csv(RAW_DIR / 'recharges.csv', index=False)
    print(f'  → {len(df)} rows written')


# ── 5. CHURN EVENTS ──────────────────────────────────────────────────────────
def generate_churn_events(msisdns, n=200):
    print(f'Generating {n} churn events...')
    churned_msisdns = random.sample(msisdns, min(n, len(msisdns)))
    records = []

    for msisdn in churned_msisdns:
        churn_date = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 730))
        record = {
            'msisdn': msisdn,
            'churn_date': churn_date.strftime('%Y-%m-%d'),
            'reason': random.choice(CHURN_REASONS),
        }
        records.append(record)

    df = pd.DataFrame(records)
    df.to_csv(RAW_DIR / 'churn_events.csv', index=False)
    print(f'  → {len(df)} rows written')


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('=' * 50)
    print('Telecom Data Generator — Egyptian Market')
    print('=' * 50)

    msisdns = generate_customers(1000)
    tower_ids = generate_cell_towers(50)
    generate_cdr(msisdns, tower_ids, 50000)
    generate_recharges(msisdns, 10000)
    generate_churn_events(msisdns, 200)

    print('=' * 50)
    print(f'All files written to: {RAW_DIR}')
    print('Ready for ETL.')