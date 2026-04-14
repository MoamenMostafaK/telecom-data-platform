# Telecom Data Platform

An end-to-end data engineering project simulating a real-world Egyptian prepaid telecom environment.

Built as a progressive portfolio piece — each layer adds a new engineering capability on top of the last.

---

## Architecture

```
generate/generate_data.py      # Synthetic data generation with realistic dirty issues
    ↓
generate/data/raw/             # Raw CSVs (customers, CDR, recharges, towers, churn)
    ↓
etl/etl.py                     # Validate → Quarantine → Load
    ↓
etl/data/quarantine/           # Rejected rows with reject reason (never silently dropped)
    ↓
PostgreSQL (Docker)            # Clean, structured data ready for analysis
    ↓
[Layer 2] dbt                  # Star schema, SCD2, ARPU models            ← coming
    ↓
[Layer 3] Airflow              # Orchestrated DAG: Extract → Load → Transform  ← coming
```

---

## Domain

Egyptian prepaid mobile market. Tables reflect real operational data:

| Table          | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `customers`    | Subscriber base. MSISDN as operational identifier per NTRA regulations |
| `cell_towers`  | Network infrastructure. Vendors: Huawei, Ericsson, Nokia               |
| `cdr`          | Call Detail Records — voice, SMS, and data sessions                    |
| `recharges`    | Prepaid top-up transactions via app, USSD, retailer, online            |
| `churn_events` | Subscriber churn log with reason tracking                              |

---

## Data Quality

Dirty issues are deliberately injected at generation to simulate real source system problems:

| Table     | Issue                         | Volume        |
| --------- | ----------------------------- | ------------- |
| customers | Invalid MSISDN format         | ~3%           |
| customers | Duplicate rows                | ~2%           |
| customers | Missing city/region           | ~5%           |
| cdr       | Dropped calls (NULL duration) | ~10% of voice |
| cdr       | Dead zone (NULL tower_id)     | ~5%           |
| cdr       | Duplicate CDR IDs             | ~3%           |
| cdr       | Inverted timestamps           | ~2%           |
| recharges | Negative amounts              | ~3%           |
| recharges | NULL channel                  | ~2%           |

The ETL validates each rule explicitly, quarantines rejected rows with a labeled reason, and logs every decision. Nothing is silently dropped.

---

## Stack

- **Python** — data generation, ETL, validation
- **PostgreSQL 16** — target database
- **Docker** — local database environment
- **pandas / numpy** — data processing
- **psycopg2** — PostgreSQL driver

---

## How to Run

**1. Start the database**

```bash
docker compose up -d
```

Wait for the healthcheck to pass. The DDL runs automatically on first start.

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

**3. Generate raw data**

```bash
python generate/generate_data.py
```

Data will be written to `generate/data/raw/`.

**4. Run ETL**

```bash
python etl/etl.py
```

Check `etl/logs/` for the run log and `etl/data/quarantine/` for rejected rows.

---

## Project Structure

```
telecom-data-platform/
├── infra/
│   └── docker-compose.yml
├── generate/
│   ├── generate_data.py   # Data generation script
│   └── data/
│       └── raw/           # Generated CSVs (customers, cdr, churn_events, cell_towers, recharges)
├── etl/
│   ├── etl.py             # ETL pipeline (validate → quarantine → load)
│   ├── logs/              # ETL run logs
│   └── data/
│       └── quarantine/    # Rejected rows with reject reason
├── sql/
│   └── ddl.sql            # DDL schema (auto-run via docker-compose)
├── scripts/
│   └── __init__.py        # Package marker (legacy utilities, may be repurposed)
├── dbt/                   # Layer 2 — coming
├── orchestration/         # Layer 3 — coming
├── .archive/              # Legacy code (preserved git history, not in use)
├── requirements.txt
└── README.md
```

---

## Project Status & Cleanup

**Recent cleanup** (April 2026):

- Consolidated duplicate ETL files → single canonical `/etl/etl.py`
- Moved data generation script → `/generate/generate_data.py`
- Unified raw data location → `/generate/data/raw/`
- Archived legacy code → `/.archive/` (git history preserved)
- Updated documentation to reflect actual structure

---

## Roadmap

- [x] Layer 1 — Data generation, PostgreSQL schema, ETL with validation and quarantine
- [ ] Layer 2 — dbt star schema (fact_cdr, dim_customers, dim_time), SCD2, ARPU models
- [ ] Layer 3 — Airflow DAG orchestrating the full pipeline on a schedule
