-- Drop existing tables if they exist
DROP TABLE IF EXISTS recharges CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS customer_plans CASCADE;
DROP TABLE IF EXISTS plans CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Plans table
CREATE TABLE plans (
  plan_id SERIAL PRIMARY KEY,
  plan_name VARCHAR(50) NOT NULL UNIQUE,
  plan_type VARCHAR(20) NOT NULL CHECK (plan_type IN ('Prepaid', 'Postpaid')),
  monthly_fee DECIMAL(10, 2) NOT NULL DEFAULT 0,
  data_limit_gb INT,
  voice_minutes INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customers table
CREATE TABLE customers (
  customer_id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  phone_number VARCHAR(20) UNIQUE,
  city VARCHAR(50),
  segment VARCHAR(20) NOT NULL CHECK (segment IN ('Prepaid', 'Postpaid')),
  email VARCHAR(100) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Suspended'))
);

-- Customer Plans (Junction table)
CREATE TABLE customer_plans (
  customer_plan_id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  plan_id INT NOT NULL REFERENCES plans(plan_id) ON DELETE CASCADE,
  start_date DATE NOT NULL DEFAULT CURRENT_DATE,
  end_date DATE,
  CONSTRAINT unique_customer_plan UNIQUE (customer_id, plan_id, start_date)
);

-- Transactions/CDR table
CREATE TABLE transactions (
  txn_id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  amount DECIMAL(10, 2) NOT NULL,
  txn_type VARCHAR(30) NOT NULL CHECK (txn_type IN ('Voice', 'SMS', 'Data', 'Payment', 'Recharge')),
  duration_seconds INT,
  status VARCHAR(20) NOT NULL CHECK (status IN ('Success', 'Failed', 'Pending')),
  transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  description VARCHAR(255)
);

-- Recharges table
CREATE TABLE recharges (
  recharge_id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  amount DECIMAL(10, 2) NOT NULL,
  method VARCHAR(20) NOT NULL CHECK (method IN ('Card', 'Cash', 'Online', 'Bank Transfer')),
  status VARCHAR(20) DEFAULT 'Success' CHECK (status IN ('Success', 'Failed', 'Pending')),
  recharge_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_txn_customer ON transactions(customer_id);
CREATE INDEX idx_txn_date ON transactions(transaction_date);
CREATE INDEX idx_recharge_customer ON recharges(customer_id);
CREATE INDEX idx_recharge_date ON recharges(recharge_date);
CREATE INDEX idx_customer_segment ON customers(segment);