--creating marts
CREATE OR ALTER VIEW marts.vw_FraudAnalytics AS
SELECT
  f.transaction_id,
  f.transaction_amount,
  f.fraud_label,
  t.transaction_datetime,
  t.transaction_date,
  t.transaction_year,
  t.transaction_month,
  c.customer_id,
  c.account_age_days,
  c.previous_fraud_flag,
  g.customer_region,
  g.is_international,
  m.merchant_category,
  d.device_id,
  ch.channel,
  ch.payment_method
FROM marts.Fact_Transactions f
LEFT JOIN curated.Dim_Customer c ON f.customer_key = c.customer_key
LEFT JOIN curated.Dim_Geo g ON f.geo_key = g.geo_key
LEFT JOIN curated.Dim_Merchant m ON f.merchant_key = m.merchant_key
LEFT JOIN curated.Dim_Device d ON f.device_key = d.device_key
LEFT JOIN curated.Dim_Channel ch ON f.channel_key = ch.channel_key
LEFT JOIN curated.Dim_Time t ON f.time_key = t.time_key;
GO

--Building Fraud Analyst SQL Pack.

--1) trends.sql - Trend Monitoring: Daily Fraud Rate & Fraud Loss
SELECT
  transaction_date,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS fraud_loss_exposure
FROM marts.vw_FraudAnalytics
GROUP BY transaction_date
ORDER BY transaction_date;
--Weekly trend:
SELECT
  DATEPART(WEEK, transaction_date) AS week_number,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS fraud_loss
FROM marts.vw_FraudAnalytics
GROUP BY DATEPART(WEEK, transaction_date)
ORDER BY week_number;

--2) velocity_windows.sql, Velocity — Multiple Transactions in Short Time
SELECT
  customer_id,
  transaction_datetime,
  COUNT(*) OVER (
    PARTITION BY customer_id
    ORDER BY transaction_datetime
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) AS txn_count_last4,
  transaction_amount,
  fraud_label
FROM marts.vw_FraudAnalytics
ORDER BY txn_count_last4 DESC;
--High-velocity suspicious:
SELECT *
FROM (
  SELECT
    customer_id,
    transaction_datetime,
    COUNT(*) OVER (
      PARTITION BY customer_id
      ORDER BY transaction_datetime
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS velocity_score,
    fraud_label
  FROM marts.vw_FraudAnalytics
) v
WHERE velocity_score >= 6
ORDER BY velocity_score DESC;

--3_anomaly_baselines.sql, Amount Anomaly vs Customer Baseline
WITH customer_avg AS (
  SELECT
    customer_id,
    AVG(transaction_amount) AS avg_amt
  FROM marts.vw_FraudAnalytics
  GROUP BY customer_id
)
SELECT
  v.customer_id,
  v.transaction_amount,
  c.avg_amt,
  (v.transaction_amount - c.avg_amt) AS deviation,
  fraud_label
FROM marts.vw_FraudAnalytics v
JOIN customer_avg c ON v.customer_id = c.customer_id
ORDER BY deviation DESC;

--4_geo_device_switch.sql, Count distinct devices per customer
WITH device_counts AS (
    SELECT
        customer_id,
        COUNT(DISTINCT device_id) AS device_count
    FROM marts.vw_FraudAnalytics
    GROUP BY customer_id
),
geo_counts AS (
    SELECT
        customer_id,
        COUNT(DISTINCT customer_region) AS region_count
    FROM marts.vw_FraudAnalytics
    GROUP BY customer_id
)
SELECT
    d.customer_id,
    d.device_count,
    g.region_count
FROM device_counts d
JOIN geo_counts g ON d.customer_id = g.customer_id
WHERE d.device_count > 1
   OR g.region_count > 1
ORDER BY d.device_count DESC, g.region_count DESC;

--5) merchant_risk.sql, Merchant Category Fraud Concentration
SELECT
  merchant_category,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS fraud_loss
FROM marts.vw_FraudAnalytics
GROUP BY merchant_category
ORDER BY fraud_loss DESC;

--6) cohorts.sql, Customer Cohort Fraud Analysis
WITH first_txn AS (
  SELECT
    customer_id,
    MIN(transaction_date) AS first_purchase_date
  FROM marts.vw_FraudAnalytics
  GROUP BY customer_id
)
SELECT
  DATEPART(MONTH, f.first_purchase_date) AS cohort_month,
  COUNT(v.transaction_id) AS total_txns,
  SUM(CASE WHEN v.fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns
FROM first_txn f
JOIN marts.vw_FraudAnalytics v ON f.customer_id = v.customer_id
GROUP BY DATEPART(MONTH, f.first_purchase_date)
ORDER BY cohort_month;

--7) rules_engine.sql, Rule-Based Fraud Flags Table
IF OBJECT_ID('marts.Rule_Flags','U') IS NOT NULL DROP TABLE marts.Rule_Flags;
GO

SELECT
  transaction_id,
  customer_id,
  transaction_amount,
  CASE WHEN transaction_amount > 10000 THEN 1 ELSE 0 END AS rule_high_amount,
  CASE WHEN is_international = 1 THEN 1 ELSE 0 END AS rule_international,
  CASE WHEN previous_fraud_flag = 1 THEN 1 ELSE 0 END AS rule_prior_fraud
INTO marts.Rule_Flags
FROM marts.vw_FraudAnalytics;
--ADDING RULE SCORE
SELECT *,
  (rule_high_amount + rule_international + rule_prior_fraud) AS total_rule_score
FROM marts.Rule_Flags
ORDER BY total_rule_score DESC;

--Investigation Summary Table
SELECT TOP 50
  r.transaction_id,
  r.customer_id,
  r.total_rule_score,
  v.transaction_amount,
  v.fraud_label
FROM (
  SELECT transaction_id,
         customer_id,
         (rule_high_amount + rule_international + rule_prior_fraud) AS total_rule_score
  FROM marts.Rule_Flags
) r
JOIN marts.vw_FraudAnalytics v ON r.transaction_id = v.transaction_id
ORDER BY total_rule_score DESC, v.transaction_amount DESC;