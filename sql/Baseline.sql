SELECT DB_NAME() AS current_db;

--Baseline: Total rows + fraud rate + fraud loss exposure
SELECT
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  SUM(CASE WHEN fraud_label = 0 THEN 1 ELSE 0 END) AS genuine_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  CAST(SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS fraud_loss_exposure
FROM dbo.stg_transactions;

--Data Quality: Missing values
SELECT
  SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN transaction_timestamp IS NULL THEN 1 ELSE 0 END) AS null_transaction_timestamp,
  SUM(CASE WHEN transaction_amount IS NULL THEN 1 ELSE 0 END) AS null_transaction_amount,
  SUM(CASE WHEN merchant_id IS NULL THEN 1 ELSE 0 END) AS null_merchant_id,
  SUM(CASE WHEN merchant_category IS NULL THEN 1 ELSE 0 END) AS null_merchant_category,
  SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS null_payment_method,
  SUM(CASE WHEN channel IS NULL THEN 1 ELSE 0 END) AS null_channel,
  SUM(CASE WHEN device_type IS NULL THEN 1 ELSE 0 END) AS null_device_type,
  SUM(CASE WHEN device_id IS NULL THEN 1 ELSE 0 END) AS null_device_id,
  SUM(CASE WHEN ip_address IS NULL THEN 1 ELSE 0 END) AS null_ip_address,
  SUM(CASE WHEN customer_age IS NULL THEN 1 ELSE 0 END) AS null_customer_age,
  SUM(CASE WHEN customer_region IS NULL THEN 1 ELSE 0 END) AS null_customer_region,
  SUM(CASE WHEN account_age_days IS NULL THEN 1 ELSE 0 END) AS null_account_age_days,
  SUM(CASE WHEN is_international IS NULL THEN 1 ELSE 0 END) AS null_is_international,
  SUM(CASE WHEN previous_fraud_flag IS NULL THEN 1 ELSE 0 END) AS null_previous_fraud_flag,
  SUM(CASE WHEN fraud_label IS NULL THEN 1 ELSE 0 END) AS null_fraud_label
FROM dbo.stg_transactions;

--Data Quality: Duplicate transaction_id
SELECT TOP 20
  transaction_id,
  COUNT(*) AS cnt
FROM dbo.stg_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

--Data Quality: Invalid values (edge cases)
SELECT
  SUM(CASE WHEN transaction_amount <= 0 THEN 1 ELSE 0 END) AS non_positive_amount,
  SUM(CASE WHEN customer_age < 18 OR customer_age > 100 THEN 1 ELSE 0 END) AS invalid_age,
  SUM(CASE WHEN account_age_days < 0 THEN 1 ELSE 0 END) AS invalid_account_age
FROM dbo.stg_transactions;

--Convert timestamp text TO datetime safely
SELECT TOP 20
  transaction_timestamp,
  TRY_CONVERT(datetime, transaction_timestamp, 120) AS ts_converted
FROM dbo.stg_transactions;

--Fraud Hotspots: 
--Fraud by channel
SELECT
  channel,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  CAST(SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS fraud_loss_exposure
FROM dbo.stg_transactions
GROUP BY channel
ORDER BY fraud_rate_pct DESC, fraud_loss_exposure DESC;

--Fraud by merchant_category
SELECT
  merchant_category,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  CAST(SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS fraud_loss_exposure
FROM dbo.stg_transactions
GROUP BY merchant_category
ORDER BY fraud_rate_pct DESC, fraud_loss_exposure DESC;

--Fraud by region
SELECT
  customer_region,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  CAST(SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS fraud_loss_exposure
FROM dbo.stg_transactions
GROUP BY customer_region
ORDER BY fraud_rate_pct DESC, fraud_loss_exposure DESC;

-- Fraud by payment method
SELECT
  payment_method,
  COUNT(*) AS total_txns,
  SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) AS fraud_txns,
  CAST(100.0 * SUM(CASE WHEN fraud_label = 1 THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS fraud_rate_pct,
  CAST(SUM(CASE WHEN fraud_label = 1 THEN transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS fraud_loss_exposure
FROM dbo.stg_transactions
GROUP BY payment_method
ORDER BY fraud_rate_pct DESC, fraud_loss_exposure DESC;

--Outliers (top 50 by amount)
SELECT TOP 50
  transaction_id, customer_id, transaction_timestamp, transaction_amount,
  channel, customer_region, merchant_category,
  is_international, previous_fraud_flag, fraud_label
FROM dbo.stg_transactions
ORDER BY transaction_amount DESC;

--Create QC tables
