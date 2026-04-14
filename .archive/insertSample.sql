-- Insert Plans
INSERT INTO plans (plan_name, plan_type, monthly_fee, data_limit_gb, voice_minutes) VALUES
('Basic Prepaid', 'Prepaid', 10.00, 5, 500),
('Premium Postpaid', 'Postpaid', 49.99, 100, 5000),
('Family Plan', 'Postpaid', 79.99, 200, 10000),
('Data Boost', 'Prepaid', 25.00, 50, 1000),
('Business', 'Postpaid', 99.99, 500, 15000);

-- Insert Customers
INSERT INTO customers (name, phone_number, city, segment, email, status) VALUES
('Ahmed Hassan', '+20101234567', 'Cairo', 'Prepaid', 'ahmed@example.com', 'Active'),
('Sara Mohamed', '+20102345678', 'Giza', 'Postpaid', 'sara@example.com', 'Active'),
('Ali Ibrahim', '+20103456789', 'Cairo', 'Postpaid', 'ali@example.com', 'Active'),
('Mona Khalil', '+20104567890', 'Alexandria', 'Prepaid', 'mona@example.com', 'Active'),
('Karim Saleh', '+20105678901', 'Cairo', 'Postpaid', 'karim@example.com', 'Active');

-- Insert Customer Plans
INSERT INTO customer_plans (customer_id, plan_id, start_date) VALUES
(1, 1, '2026-01-01'),
(2, 2, '2026-01-01'),
(3, 3, '2025-12-15'),
(4, 4, '2026-02-01'),
(5, 5, '2025-11-01');

-- Insert Transactions
INSERT INTO transactions (customer_id, amount, txn_type, duration_seconds, status, description) VALUES
(1, 50.00, 'Recharge', NULL, 'Success', 'Card recharge'),
(2, 150.50, 'Talk Time', 3600, 'Success', 'Voice call - 60 minutes'),
(3, 100.00, 'Data', 7200, 'Success', 'Data usage 5GB'),
(4, 25.00, 'Recharge', NULL, 'Failed', 'Card declined'),
(5, 200.00, 'Payment', NULL, 'Success', 'Monthly bill payment');

-- Insert Recharges
INSERT INTO recharges (customer_id, amount, method, status) VALUES
(1, 50.00, 'Card', 'Success'),
(2, 100.00, 'Online', 'Success'),
(3, 70.00, 'Card', 'Success'),
(4, 30.00, 'Cash', 'Success'),
(5, 150.00, 'Bank Transfer', 'Success');