-- ============================================
-- Create schema
-- ============================================

CREATE SCHEMA IF NOT EXISTS telecom;

SET search_path TO telecom;

-- ============================================
-- Subscribers table
-- ============================================

CREATE TABLE IF NOT EXISTS subscribers (
    customer_id     INT PRIMARY KEY,
    name            TEXT NOT NULL,
    plan            TEXT CHECK (plan IN ('prepaid','postpaid','corporate')),
    region          TEXT,
    signup_date     DATE,
    status          TEXT CHECK (status IN ('active','churned','suspended')),
    referred_by     INT
);

-- ============================================
-- Calls table
-- ============================================

CREATE TABLE IF NOT EXISTS calls (
    call_id         BIGINT PRIMARY KEY,
    caller_id       INT,
    receiver_id     INT,
    call_date       TIMESTAMP,
    duration_sec    INT,
    call_type       TEXT CHECK (call_type IN ('local','intl','roaming')),
    cost            NUMERIC(10,2)
);

-- ============================================
-- Data Usage
-- ============================================

CREATE TABLE IF NOT EXISTS data_usage (
    usage_id        BIGINT PRIMARY KEY,
    subscriber_id   INT,
    usage_date      DATE,
    mb_used         NUMERIC,
    plan_mb         NUMERIC
);

-- ============================================
-- Dimension table (SCD Type 2)
-- ============================================

CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    surrogate_key   BIGSERIAL PRIMARY KEY,
    customer_id     INT,
    name            TEXT,
    city            TEXT,
    segment         TEXT,
    effective_date  DATE,
    expiry_date     DATE,
    is_current      BOOLEAN
);

-- ============================================
-- Fact table
-- ============================================

CREATE TABLE IF NOT EXISTS fact_recharges (
    txn_id          BIGINT PRIMARY KEY,
    surrogate_key   INT,
    txn_date        DATE,
    amount          NUMERIC(10,2),
    recharge_type   TEXT
);

-- ============================================
-- Pipeline audit table
-- ============================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    batch_id        BIGINT,
    step_name       TEXT,
    rows_in         INT,
    rows_out        INT,
    status          TEXT,
    run_time        TIMESTAMP,
    error_msg       TEXT
);