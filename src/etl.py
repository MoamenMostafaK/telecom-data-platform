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
BASE_DIR = Path(__file__).parent.parent 
RAW_DIR = BASE_DIR / 'generate' / 'data' / 'raw'
QUARANTINE_DIR = BASE_DIR / 'etl' / 'data' / 'quarantine'
LOG_DIR = BASE_DIR / 'etl' / 'logs'

QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    'host':     'localhost',
    'port':     5498,        # The external port from your docker-compose
    'dbname':   'telecom_db', 
    'user':     'postgres',
    'password': 'postgres',
}

# =============================================================================
# LOGGING & AUDIT HELPERS
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

def log_run_start(conn, run_id):
    """Initializes audit record in the database."""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO etl_log (run_id, status) VALUES (%s, 'RUNNING')", (run_id,))
    conn.commit()

def log_run_end(conn, run_id, status, processed, quarantined):
    """Finalizes audit record with telemetry."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE etl_log 
            SET status = %s, end_time = NOW(), rows_processed = %s, rows_quarantined = %s
            WHERE run_id = %s
        """, (status, processed, quarantined, run_id))
    conn.commit()

# =============================================================================
# SHARED ETL HELPERS
# =============================================================================
def quarantine(df: pd.DataFrame, table: str, reason_col: str = '_reject_reason'):
    """Write rejected rows to quarantine CSV and return count."""
    if df.empty:
        return 0
    out = QUARANTINE_DIR / f'{table}_{run_id}.csv'
    df.to_csv(out, index=False)
    log.warning(f'QUARANTINE | {table} | {len(df)} rows → {out.name}')
    return len(df)

def load_to_postgres(df: pd.DataFrame, table: str, conn, cols: list):
    """Bulk insert clean rows and return count."""
    if df.empty:
        log.info(f'LOAD | {table} | 0 rows')
        return 0

    df_payload = df[cols].copy()
    records = [tuple(None if pd.isna(v) else v for v in row) for row in df_payload.itertuples(index=False)]
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

# =============================================================================
# TABLE PROCESSORS
# =============================================================================

def process_customers(conn):
    log.info('--- CUSTOMERS ---')
    df = pd.read_csv(RAW_DIR / 'customers.csv', dtype={'msisdn': str, 'national_id': str})
    df['_reject_reason'] = None

    # Logic: Validation & Deduplication
    invalid_msisdn = ~df['msisdn'].apply(is_valid_msisdn)
    df.loc[invalid_msisdn, '_reject_reason'] = 'invalid_msisdn'
    
    dupes = df.duplicated(keep='first')
    df.loc[dupes & df['_reject_reason'].isna(), '_reject_reason'] = 'duplicate_row'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    q_count = quarantine(rejected, 'customers')
    cols = ['customer_id', 'msisdn', 'full_name', 'city', 'region', 'national_id', 'registration_date', 'status', 'balance_egp']
    l_count = load_to_postgres(clean, 'customers', conn, cols)

    return set(clean['msisdn'].unique()), l_count, q_count

def process_cell_towers(conn):
    log.info('--- CELL TOWERS ---')
    df = pd.read_csv(RAW_DIR / 'cell_towers.csv')
    df['_reject_reason'] = None

    bad_coords = (~df['latitude'].between(22.0, 31.5)) | (~df['longitude'].between(25.0, 37.0))
    df.loc[bad_coords, '_reject_reason'] = 'coordinates_out_of_egypt'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    q_count = quarantine(rejected, 'cell_towers')
    cols = ['tower_id', 'city', 'region', 'latitude', 'longitude', 'technology', 'vendor']
    l_count = load_to_postgres(clean, 'cell_towers', conn, cols)

    return set(clean['tower_id'].unique()), l_count, q_count

def process_recharges(conn, valid_msisdns):
    log.info('--- RECHARGES ---')
    df = pd.read_csv(RAW_DIR / 'recharges.csv', dtype={'msisdn': str})
    df['_reject_reason'] = None

    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn, '_reject_reason'] = 'msisdn_not_in_customers'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    q_count = quarantine(rejected, 'recharges')
    cols = ['recharge_id', 'msisdn', 'amount_egp', 'recharge_timestamp', 'channel']
    l_count = load_to_postgres(clean, 'recharges', conn, cols)
    return l_count, q_count

def process_cdr(conn, valid_msisdns, valid_tower_ids):
    log.info('--- CDR ---')
    df = pd.read_csv(RAW_DIR / 'cdr.csv', dtype={'msisdn': str, 'called_msisdn': str})
    df['_reject_reason'] = None

    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn, '_reject_reason'] = 'msisdn_not_in_customers'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    q_count = quarantine(rejected, 'cdr')
    cols = ['cdr_id', 'msisdn', 'called_msisdn', 'tower_id', 'record_type', 'duration_seconds', 'data_mb', 'cost_egp', 'start_timestamp', 'end_timestamp']
    l_count = load_to_postgres(clean, 'cdr', conn, cols)
    return l_count, q_count

def process_churn_events(conn, valid_msisdns):
    log.info('--- CHURN EVENTS ---')
    df = pd.read_csv(RAW_DIR / 'churn_events.csv', dtype={'msisdn': str})
    df['_reject_reason'] = None

    unknown_msisdn = ~df['msisdn'].isin(valid_msisdns)
    df.loc[unknown_msisdn, '_reject_reason'] = 'msisdn_not_in_customers'

    rejected = df[df['_reject_reason'].notna()].copy()
    clean = df[df['_reject_reason'].isna()].drop(columns=['_reject_reason'])

    q_count = quarantine(rejected, 'churn_events')
    cols = ['msisdn', 'churn_date', 'reason']
    l_count = load_to_postgres(clean, 'churn_events', conn, cols)
    return l_count, q_count

# =============================================================================
# MAIN PIPELINE
# =============================================================================
def main():
    log.info('=' * 60)
    log.info(f'ETL PROCESS INITIATED | ID: {run_id}')
    log.info('=' * 60)

    total_l = 0
    total_q = 0
    status = 'FAILED'
    conn = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log_run_start(conn, run_id)

        # Execute processors & track counts
        msisdns, l1, q1 = process_customers(conn)
        towers, l2, q2 = process_cell_towers(conn)
        l3, q3 = process_recharges(conn, msisdns)
        l4, q4 = process_cdr(conn, msisdns, towers)
        l5, q5 = process_churn_events(conn, msisdns)

        total_l = l1 + l2 + l3 + l4 + l5
        total_q = q1 + q2 + q3 + q4 + q5
        
        status = 'SUCCESS'
        log.info('=' * 60)
        log.info(f'ETL COMPLETE | Loaded: {total_l} | Quarantined: {total_q}')
        log.info('=' * 60)

    except Exception as e:
        log.error(f'PIPELINE FAILURE: {e}')
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            log_run_end(conn, run_id, status, total_l, total_q)
            conn.close()
            log.info("Database connection closed.")

if __name__ == '__main__':
    main()