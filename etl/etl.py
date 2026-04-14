import pandas as pd
import numpy as np
import psycopg2
import logging
import re
import os
from pathlib import Path
from datetime import datetime

# =============================================================================
# CONFIG
# =============================================================================
BASE_DIR = Path(__file__).parent.parent # Go up to telecom-data-platform
RAW_DIR = BASE_DIR / 'generate' / 'data' / 'raw'
QUARANTINE_DIR = BASE_DIR / 'etl' / 'data' / 'quarantine'
LOG_DIR        = BASE_DIR / 'etl' / 'logs'

QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     os.getenv('DB_PORT', '5498'),    
    'dbname':   os.getenv('DB_NAME', 'telecom'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

# =============================================================================
# LOGGING
# =============================================================================
run_id = datetime.now().strftime('%Y%m%d_%H%M%S')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f'etl_{run_id}.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# =============================================================================
# HELPERS
# =============================================================================
def quarantine(df: pd.DataFrame, table: str, reason_col: str = '_reject_reason'):
    """Write rejected rows to quarantine CSV with run_id and reason."""
    if df.empty:
        return
    out = QUARANTINE_DIR / f'{table}_{run_id}.csv'
    df.to_csv(out, index=False)
    log.warning(f'QUARANTINE | {table} | {len(df)} rows → {out.name}')
    reasons = df[reason_col].value_counts().to_dict()
    for reason, count in reasons.items():
        log.warning(f'  └─ {reason}: {count}')


def load_to_postgres(df: pd.DataFrame, table: str, conn, cols: list):
    """Bulk insert clean rows using executemany."""
    if df.empty:
        log.info(f'LOAD | {table} | 0 rows (nothing clean to load)')
        return 0

    df = df[cols]
    records = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]
    placeholders = ', '.join(['%s'] * len(cols))
    col_names = ', '.join(cols)

    with conn.cursor() as cur:
        cur.executemany(
            f'INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
            records
        )
    conn.commit()
    log.info(f'LOAD | {table} | {len(records)} rows inserted')
    return len(records)


# =============================================================================
# VALIDATION RULES
# =============================================================================
MSISDN_PATTERN = re.compile(r'^\+2(010|011|012|015)\d{8}$')

def is_valid_msisdn(val):
    return bool(MSISDN_PATTERN.match(str(val))) if pd.notna(val) else False

def is_valid_national_id(val):
    return bool(re.match(r'^\d{14}$', str(val))) if pd.notna(val) else False


# =============================================================================
# TABLE PROCESSORS
# =============================================================================

def process_customers(conn) -> set:
    """
    Returns set of valid MSISDNs loaded — needed by downstream tables.
    Rules:
      - MSISDN must match Egyptian format
      - Duplicate rows removed (keep first)
      - status must be valid
    """
    log.info('--- CUSTOMERS ---')
    df = pd.read_csv(RAW_DIR / 'customers.csv', dtype={'msisdn': str, 'national_id': str})
    log.info(f'Raw rows: {len(df)}')

    df['_reject_reason'] = None

    # Rule 1: invalid MSISDN
    invalid_msisdn = ~df['msisdn'].apply(is_valid_msisdn)
    df.loc[invalid_msisdn & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_msisdn'

    # Rule 2: duplicate rows (keep first occurrence)
    dupes = df.duplicated(keep='first')
    df.loc[dupes & df['_reject_reason'].isna(), '_reject_reason'] = 'duplicate_row'

    # Rule 3: invalid status
    valid_statuses = {'active', 'suspended', 'churned'}
    bad_status = ~df['status'].isin(valid_statuses)
    df.loc[bad_status & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_status'

    # Split
    rejected = df[df['_reject_reason'].notna()].copy()
    clean    = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    quarantine(rejected, 'customers')
    log.info(f'Clean rows: {len(clean)} | Rejected: {len(rejected)}')

    cols = ['customer_id', 'msisdn', 'full_name', 'city', 'region',
            'national_id', 'registration_date', 'status', 'balance_egp']
    load_to_postgres(clean, 'customers', conn, cols)

    return set(clean['msisdn'].unique())


def process_cell_towers(conn) -> set:
    """
    Returns set of valid tower_ids loaded.
    Rules:
      - latitude must be within Egypt bounds (22–31.5)
      - longitude must be within Egypt bounds (25–37)
      - technology and vendor must be valid
    """
    log.info('--- CELL TOWERS ---')
    df = pd.read_csv(RAW_DIR / 'cell_towers.csv')
    log.info(f'Raw rows: {len(df)}')

    df['_reject_reason'] = None

    # Rule 1: coordinates out of Egypt bounds
    bad_lat = ~df['latitude'].between(22.0, 31.5)
    bad_lon = ~df['longitude'].between(25.0, 37.0)
    bad_coords = bad_lat | bad_lon
    df.loc[bad_coords & df['_reject_reason'].isna(), '_reject_reason'] = 'coordinates_out_of_egypt'

    # Rule 2: invalid technology
    bad_tech = ~df['technology'].isin(['2G', '3G', '4G', '5G'])
    df.loc[bad_tech & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_technology'

    # Rule 3: invalid vendor
    bad_vendor = ~df['vendor'].isin(['Huawei', 'Ericsson', 'Nokia'])
    df.loc[bad_vendor & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_vendor'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean    = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    quarantine(rejected, 'cell_towers')
    log.info(f'Clean rows: {len(clean)} | Rejected: {len(rejected)}')

    cols = ['tower_id', 'city', 'region', 'latitude', 'longitude', 'technology', 'vendor']
    load_to_postgres(clean, 'cell_towers', conn, cols)

    return set(clean['tower_id'].unique())


def process_recharges(conn, valid_msisdns: set):
    """
    Rules:
      - MSISDN must exist in customers (referential integrity)
      - amount_egp must be positive
      - channel must be valid if present
    """
    log.info('--- RECHARGES ---')
    df = pd.read_csv(RAW_DIR / 'recharges.csv', dtype={'msisdn': str})
    log.info(f'Raw rows: {len(df)}')

    df['_reject_reason'] = None

    # Rule 1: MSISDN not in customers
    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn & df['_reject_reason'].isna(), '_reject_reason'] = 'msisdn_not_in_customers'

    # Rule 2: negative or zero amount
    bad_amount = df['amount_egp'] <= 0
    df.loc[bad_amount & df['_reject_reason'].isna(), '_reject_reason'] = 'negative_or_zero_amount'

    # Rule 3: invalid channel (NULLs allowed — unknown channel, not a hard reject)
    valid_channels = {'app', 'ussd', 'retailer', 'online'}
    bad_channel = df['channel'].notna() & ~df['channel'].isin(valid_channels)
    df.loc[bad_channel & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_channel'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean    = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    quarantine(rejected, 'recharges')
    log.info(f'Clean rows: {len(clean)} | Rejected: {len(rejected)}')

    cols = ['recharge_id', 'msisdn', 'amount_egp', 'recharge_timestamp', 'channel']
    load_to_postgres(clean, 'recharges', conn, cols)


def process_cdr(conn, valid_msisdns: set, valid_tower_ids: set):
    """
    Rules:
      - MSISDN must exist in customers
      - duplicate cdr_id removed
      - end_timestamp must be >= start_timestamp
      - duration_seconds NULL allowed for voice (dropped call) — not a reject
      - tower_id NULL allowed (dead zone) — not a reject
      - cost_egp must be >= 0
    """
    log.info('--- CDR ---')
    df = pd.read_csv(RAW_DIR / 'cdr.csv', dtype={'msisdn': str, 'called_msisdn': str})
    log.info(f'Raw rows: {len(df)}')

    df['_reject_reason'] = None

    # Rule 1: MSISDN not in customers
    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn & df['_reject_reason'].isna(), '_reject_reason'] = 'msisdn_not_in_customers'

    # Rule 2: duplicate cdr_id (keep first)
    dupes = df.duplicated(subset=['cdr_id'], keep='first')
    df.loc[dupes & df['_reject_reason'].isna(), '_reject_reason'] = 'duplicate_cdr_id'

    # Rule 3: inverted timestamps
    bad_ts = df['end_timestamp'] < df['start_timestamp']
    df.loc[bad_ts & df['_reject_reason'].isna(), '_reject_reason'] = 'end_before_start_timestamp'

    # Rule 4: negative cost
    bad_cost = df['cost_egp'] < 0
    df.loc[bad_cost & df['_reject_reason'].isna(), '_reject_reason'] = 'negative_cost'

    # Rule 5: tower_id not in cell_towers (if not NULL — NULLs are dead zones, allowed)
    bad_tower = df['tower_id'].notna() & ~df['tower_id'].isin(valid_tower_ids)
    df.loc[bad_tower & df['_reject_reason'].isna(), '_reject_reason'] = 'tower_id_not_in_cell_towers'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean    = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    quarantine(rejected, 'cdr')
    log.info(f'Clean rows: {len(clean)} | Rejected: {len(rejected)}')

    cols = ['cdr_id', 'msisdn', 'called_msisdn', 'tower_id', 'record_type',
            'duration_seconds', 'data_mb', 'cost_egp', 'start_timestamp', 'end_timestamp']
    load_to_postgres(clean, 'cdr', conn, cols)


def process_churn_events(conn, valid_msisdns: set):
    """
    Rules:
      - MSISDN must exist in customers
      - reason must be valid
      - duplicate (msisdn, churn_date) removed
    """
    log.info('--- CHURN EVENTS ---')
    df = pd.read_csv(RAW_DIR / 'churn_events.csv', dtype={'msisdn': str})
    log.info(f'Raw rows: {len(df)}')

    df['_reject_reason'] = None

    # Rule 1: MSISDN not in customers
    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn & df['_reject_reason'].isna(), '_reject_reason'] = 'msisdn_not_in_customers'

    # Rule 2: invalid reason
    bad_reason = ~df['reason'].isin(['voluntary', 'involuntary', 'ported'])
    df.loc[bad_reason & df['_reject_reason'].isna(), '_reject_reason'] = 'invalid_churn_reason'

    # Rule 3: duplicate (msisdn, churn_date)
    dupes = df.duplicated(subset=['msisdn', 'churn_date'], keep='first')
    df.loc[dupes & df['_reject_reason'].isna(), '_reject_reason'] = 'duplicate_churn_event'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean    = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    quarantine(rejected, 'churn_events')
    log.info(f'Clean rows: {len(clean)} | Rejected: {len(rejected)}')

    cols = ['msisdn', 'churn_date', 'reason']
    load_to_postgres(clean, 'churn_events', conn, cols)


# =============================================================================
# MAIN
# =============================================================================
def main():
    log.info('=' * 60)
    log.info(f'ETL START | run_id={run_id}')
    log.info('=' * 60)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info('Database connection established')
    except Exception as e:
        log.error(f'Failed to connect to database: {e}')
        raise

    try:
        # Order matters — customers and towers must load before CDR/recharges
        valid_msisdns  = process_customers(conn)
        valid_tower_ids = process_cell_towers(conn)
        process_recharges(conn, valid_msisdns)
        process_cdr(conn, valid_msisdns, valid_tower_ids)
        process_churn_events(conn, valid_msisdns)

        log.info('=' * 60)
        log.info('ETL COMPLETE')
        log.info('=' * 60)

    except Exception as e:
        log.error(f'ETL FAILED: {e}')
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()