-- The Plan's Day 3 15 queries for telecom data platform:
-- 1. Active Customers for a specific region:
SELECT customer_id, name, region, signup_date, churn_date
FROM customers
WHERE region = 'North' AND churn_date IS NULL   ; 

--2 Total Customer base and churned customers by region:
SELECT 
    region, 
    COUNT(customer_id) AS total_customers, 
    SUM(
        CASE WHEN churn_date IS NOT NULL 
        THEN 1 ELSE 0 END
                    ) AS churned_customers 
FROM customers 
GROUP BY region;

--3 Premimum Plan Customers:
SELECT 
    c.name, 
    p.plan_name, 
    p.monthly_fee 
FROM customers c 
JOIN plans p ON c.plan_id = p.plan_id 
WHERE p.monthly_fee >= 75.00 
  AND c.churn_date IS NULL;

--4 Monthly Recurring Revenue (MRR) by Plan:
  SELECT 
    p.plan_name, 
    SUM(p.monthly_fee) AS total_mrr 
FROM customers c 
JOIN plans p ON c.plan_id = p.plan_id 
WHERE c.churn_date IS NULL 
GROUP BY p.plan_name;


--5- Average Minutes per Call Type:
SELECT 
    call_type, 
    AVG(duration_sec) / 60.0 AS avg_duration_minutes 
FROM cdr 
GROUP BY call_type;


--6 Categorize Customers by data allowance on their plan:
SELECT 
    c.name, 
    p.data_gb, 
    CASE 
        WHEN p.data_gb >= 50 THEN 'High Volume' 
        WHEN p.data_gb BETWEEN 15 AND 49 THEN 'Standard' 
        ELSE 'Low Volume' 
    END AS data_tier 
FROM customers c 
JOIN plans p ON c.plan_id = p.plan_id;

--7 Identify towers with high volume of call records (100 will be relatively for a set of 2000 records in cdr):
SELECT 
    tower_id, 
    COUNT(record_id) AS total_calls 
FROM cdr 
GROUP BY tower_id 
HAVING COUNT(record_id) > 100;

--8 Churn Rate Count and Percentage by Plan:  
SELECT 
    p.plan_name, 
    COUNT(c.customer_id) AS total_signups, 
  	SUM(CASE WHEN c.churn_date IS NOT NULL THEN 1 ELSE 0 END) AS churned_customers,
    SUM(CASE WHEN c.churn_date IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.customer_id) AS churn_rate_pct 
FROM plans p 
JOIN customers c ON p.plan_id = c.plan_id 
GROUP BY p.plan_name;

--9 Aggregates Event Count for Each tower with severity in the last 30 days:
SELECT 
    tower_id, 
    severity, 
    COUNT(event_id) AS event_count 
FROM network_events 
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days' 
GROUP BY 1, 2
order by 1, 2 ;

