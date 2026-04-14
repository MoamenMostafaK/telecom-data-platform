-- =============================================================================
-- Telecom Data Platform — PostgreSQL DDL
-- Egyptian Prepaid Market
-- =============================================================================

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS churn_events;
DROP TABLE IF EXISTS cdr;
DROP TABLE IF EXISTS recharges;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS cell_towers;

-- =============================================================================
-- CUSTOMERS
-- =============================================================================
CREATE TABLE customers (
    customer_id         UUID            PRIMARY KEY,
    msisdn              VARCHAR(20)     NOT NULL UNIQUE,
    full_name           VARCHAR(100)    NOT NULL,
    city                VARCHAR(50),                        -- nullable: some records missing
    region              VARCHAR(50),                        -- nullable: some records missing
    national_id         VARCHAR(14),                        -- NTRA regulation: 14 digits
    registration_date   DATE            NOT NULL,
    status              VARCHAR(20)     NOT NULL CHECK (status IN ('active', 'suspended', 'churned')),
    balance_egp         NUMERIC(10, 2)  DEFAULT 0.00,
    created_at          TIMESTAMP       DEFAULT NOW()
);

-- Index on msisdn — most common join key in telecom
CREATE INDEX idx_customers_msisdn ON customers(msisdn);
CREATE INDEX idx_customers_status ON customers(status);

COMMENT ON TABLE customers IS 'Prepaid subscriber base. MSISDN is the operational identifier per NTRA regulations.';
COMMENT ON COLUMN customers.national_id IS 'Egyptian National ID — 14 digits. Required by NTRA for SIM registration.';
COMMENT ON COLUMN customers.balance_egp IS 'Current prepaid balance in Egyptian Pounds.';


-- =============================================================================
-- CELL TOWERS
-- =============================================================================
CREATE TABLE cell_towers (
    tower_id            UUID            PRIMARY KEY,
    city                VARCHAR(50)     NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    latitude            NUMERIC(9, 6)   NOT NULL,
    longitude           NUMERIC(9, 6)   NOT NULL,
    technology          VARCHAR(5)      NOT NULL CHECK (technology IN ('2G', '3G', '4G', '5G')),
    vendor              VARCHAR(20)     NOT NULL CHECK (vendor IN ('Huawei', 'Ericsson', 'Nokia')),
    created_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX idx_towers_region ON cell_towers(region);
CREATE INDEX idx_towers_technology ON cell_towers(technology);

COMMENT ON TABLE cell_towers IS 'Network infrastructure. Vendors reflect real Egyptian market: Huawei, Ericsson, Nokia.';


-- =============================================================================
-- RECHARGES
-- =============================================================================
CREATE TABLE recharges (
    recharge_id         UUID            PRIMARY KEY,
    msisdn              VARCHAR(20)     NOT NULL REFERENCES customers(msisdn),
    amount_egp          NUMERIC(10, 2)  NOT NULL CHECK (amount_egp > 0),
    recharge_timestamp  TIMESTAMP       NOT NULL,
    channel             VARCHAR(20)     CHECK (channel IN ('app', 'ussd', 'retailer', 'online')),
    created_at          TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX idx_recharges_msisdn ON recharges(msisdn);
CREATE INDEX idx_recharges_timestamp ON recharges(recharge_timestamp);
CREATE INDEX idx_recharges_channel ON recharges(channel);

COMMENT ON TABLE recharges IS 'Prepaid top-up transactions. Negative amounts and NULL channels are quarantined at ETL.';
COMMENT ON COLUMN recharges.channel IS 'Top-up channel: app (Vodafone/Orange app), ussd (*recharge#), retailer, online.';


-- =============================================================================
-- CDR (Call Detail Records)
-- =============================================================================
CREATE TABLE cdr (
    cdr_id              UUID            PRIMARY KEY,
    msisdn              VARCHAR(20)     NOT NULL REFERENCES customers(msisdn),
    called_msisdn       VARCHAR(20),                        -- NULL for data sessions
    tower_id            UUID            REFERENCES cell_towers(tower_id),  -- NULL = dead zone
    record_type         VARCHAR(10)     NOT NULL CHECK (record_type IN ('voice', 'sms', 'data')),
    duration_seconds    INTEGER         CHECK (duration_seconds >= 0),     -- NULL = dropped call
    data_mb             NUMERIC(10, 3)  CHECK (data_mb >= 0),              -- NULL for voice/sms
    cost_egp            NUMERIC(10, 4)  NOT NULL CHECK (cost_egp >= 0),
    start_timestamp     TIMESTAMP       NOT NULL,
    end_timestamp       TIMESTAMP       NOT NULL,
    created_at          TIMESTAMP       DEFAULT NOW(),

    -- Logical constraint: end must be after start
    CONSTRAINT chk_timestamps CHECK (end_timestamp >= start_timestamp)
);

CREATE INDEX idx_cdr_msisdn ON cdr(msisdn);
CREATE INDEX idx_cdr_tower ON cdr(tower_id);
CREATE INDEX idx_cdr_record_type ON cdr(record_type);
CREATE INDEX idx_cdr_start_timestamp ON cdr(start_timestamp);

COMMENT ON TABLE cdr IS 'Call Detail Records — one row per voice call, SMS, or data session.';
COMMENT ON COLUMN cdr.duration_seconds IS 'NULL indicates dropped call (no clean termination).';
COMMENT ON COLUMN cdr.tower_id IS 'NULL indicates dead zone — no tower association possible.';
COMMENT ON COLUMN cdr.data_mb IS 'Only populated for data sessions. NULL for voice and SMS.';


-- =============================================================================
-- CHURN EVENTS
-- =============================================================================
CREATE TABLE churn_events (
    msisdn              VARCHAR(20)     NOT NULL REFERENCES customers(msisdn),
    churn_date          DATE            NOT NULL,
    reason              VARCHAR(20)     NOT NULL CHECK (reason IN ('voluntary', 'involuntary', 'ported')),
    created_at          TIMESTAMP       DEFAULT NOW(),

    PRIMARY KEY (msisdn, churn_date)  -- a customer can churn, return, churn again
);

CREATE INDEX idx_churn_msisdn ON churn_events(msisdn);
CREATE INDEX idx_churn_date ON churn_events(churn_date);
CREATE INDEX idx_churn_reason ON churn_events(reason);

COMMENT ON TABLE churn_events IS 'Subscriber churn log. Composite PK allows multiple churn events per MSISDN over time.';
COMMENT ON COLUMN churn_events.reason IS 'voluntary=customer choice, involuntary=non-payment, ported=moved to competitor.';