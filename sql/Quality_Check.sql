SELECT DB_NAME() AS current_db;
SELECT COUNT(*) AS staging_count FROM staging.stg_transactions;
SELECT TOP 5 * FROM staging.stg_transactions;

--Create QC tables in SQL
IF OBJECT_ID('curated.QC_Summary','U') IS NOT NULL DROP TABLE curated.QC_Summary;
GO
CREATE TABLE curated.QC_Summary (
    qc_run_id INT NOT NULL,
    qc_dt DATETIME NOT NULL DEFAULT GETDATE(),
    rule_name VARCHAR(200) NOT NULL,
    failed_count INT NOT NULL,
    notes VARCHAR(500) NULL
);
--Define QC rules and measure failures
--Invalid timestamps
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT
  1,
  'Invalid timestamp (TRY_CONVERT failed)',
  SUM(CASE WHEN TRY_CONVERT(datetime, transaction_timestamp, 120) IS NULL THEN 1 ELSE 0 END),
  'transaction_timestamp cannot be parsed with style 120'
FROM staging.stg_transactions;
--Non-positive or null amount
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT
  1,
  'Non-positive or NULL transaction_amount',
  SUM(CASE WHEN transaction_amount IS NULL OR transaction_amount <= 0 THEN 1 ELSE 0 END),
  'amount must be > 0'
FROM staging.stg_transactions;
--Impossible ages
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT
  1,
  'Impossible or NULL customer_age (<18 or >100 or NULL)',
  SUM(CASE WHEN customer_age IS NULL OR customer_age < 18 OR customer_age > 100 THEN 1 ELSE 0 END),
  'age must be 18..100; else set NULL in clean layer'
FROM staging.stg_transactions;
--Duplicate transaction_id
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT
  1,
  'Duplicate transaction_id',
  ISNULL(SUM(dup_cnt - 1),0),
  'duplicates beyond first occurrence'
FROM (
  SELECT transaction_id, COUNT(*) AS dup_cnt
  FROM staging.stg_transactions
  GROUP BY transaction_id
  HAVING COUNT(*) > 1
) d;
--Missing critical keys
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT
  1,
  'Missing critical keys (customer_id/merchant_id/device_id)',
  SUM(CASE WHEN customer_id IS NULL OR merchant_id IS NULL OR device_id IS NULL THEN 1 ELSE 0 END),
  'critical identifiers should not be null'
FROM staging.stg_transactions;
--View QC report
SELECT * FROM curated.QC_Summary WHERE qc_run_id = 1 ORDER BY qc_dt;

--Build Clean Curated Transactions Table
IF OBJECT_ID('curated.Clean_Transactions','U') IS NOT NULL DROP TABLE curated.Clean_Transactions;
GO

WITH base AS (
  SELECT
    *,
    TRY_CONVERT(datetime, transaction_timestamp, 120) AS tx_dt
  FROM staging.stg_transactions
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY tx_dt DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  customer_id,
  tx_dt AS transaction_datetime,
  transaction_amount,
  currency,
  merchant_id,
  merchant_category,
  payment_method,
  channel,
  device_type,
  device_id,
  ip_address,
  CASE WHEN customer_age BETWEEN 18 AND 100 THEN customer_age ELSE NULL END AS customer_age,
  customer_region,
  account_age_days,
  is_international,
  previous_fraud_flag,
  fraud_label,
  GETDATE() AS ingestion_dt,
  'FraudCSV' AS source_system,
  1 AS load_batch_id
INTO curated.Clean_Transactions
FROM dedup
WHERE rn = 1
  AND tx_dt IS NOT NULL
  AND transaction_amount IS NOT NULL
  AND transaction_amount > 0;
GO
--Validating
SELECT COUNT(*) AS raw_count FROM staging.stg_transactions;
SELECT COUNT(*) AS clean_count FROM curated.Clean_Transactions;
--Log After Cleaning count
INSERT INTO curated.QC_Summary (qc_run_id, rule_name, failed_count, notes)
SELECT 1, 'Clean_Transactions rowcount', COUNT(*), 'rows after Phase 3 cleaning rules'
FROM curated.Clean_Transactions;

SELECT * FROM curated.QC_Summary WHERE qc_run_id = 1 ORDER BY qc_dt;
--Rebuild Dimensions from CLEAN
TRUNCATE TABLE curated.Dim_Time;
TRUNCATE TABLE curated.Dim_Channel;
TRUNCATE TABLE curated.Dim_Device;
TRUNCATE TABLE curated.Dim_Merchant;
TRUNCATE TABLE curated.Dim_Geo;
TRUNCATE TABLE curated.Dim_Customer;
GO

INSERT INTO curated.Dim_Customer (customer_id, customer_age, account_age_days, previous_fraud_flag, source_system, load_batch_id)
SELECT DISTINCT customer_id, customer_age, account_age_days, previous_fraud_flag, 'FraudCSV', 1
FROM curated.Clean_Transactions;

INSERT INTO curated.Dim_Geo (customer_region, is_international, source_system, load_batch_id)
SELECT DISTINCT customer_region, is_international, 'FraudCSV', 1
FROM curated.Clean_Transactions;

INSERT INTO curated.Dim_Merchant (merchant_id, merchant_category, source_system, load_batch_id)
SELECT DISTINCT merchant_id, merchant_category, 'FraudCSV', 1
FROM curated.Clean_Transactions;

INSERT INTO curated.Dim_Device (device_id, device_type, ip_address, source_system, load_batch_id)
SELECT DISTINCT device_id, device_type, ip_address, 'FraudCSV', 1
FROM curated.Clean_Transactions;

INSERT INTO curated.Dim_Channel (channel, payment_method, source_system, load_batch_id)
SELECT DISTINCT channel, payment_method, 'FraudCSV', 1
FROM curated.Clean_Transactions;

INSERT INTO curated.Dim_Time (transaction_datetime, transaction_date, transaction_hour, transaction_month, transaction_year, source_system, load_batch_id)
SELECT DISTINCT
  transaction_datetime,
  CAST(transaction_datetime AS date),
  DATEPART(hour, transaction_datetime),
  DATEPART(month, transaction_datetime),
  DATEPART(year, transaction_datetime),
  'FraudCSV',
  1
FROM curated.Clean_Transactions;
GO
--Rebuild Fact Table from CLEAN
TRUNCATE TABLE marts.Fact_Transactions;
GO

INSERT INTO marts.Fact_Transactions (
    transaction_id, customer_key, geo_key, merchant_key, device_key, channel_key, time_key,
    transaction_amount, fraud_label, record_hash, source_system, load_batch_id
)
SELECT
    s.transaction_id,
    c.customer_key,
    g.geo_key,
    m.merchant_key,
    d.device_key,
    ch.channel_key,
    t.time_key,
    s.transaction_amount,
    s.fraud_label,
    CONVERT(VARCHAR(100),
        HASHBYTES('SHA1',
            CONCAT(s.transaction_id, s.customer_id, s.merchant_id, s.transaction_amount, CONVERT(VARCHAR(19), s.transaction_datetime, 120))
        ), 2
    ) AS record_hash,
    'FraudCSV',
    1
FROM curated.Clean_Transactions s
LEFT JOIN curated.Dim_Customer c ON s.customer_id = c.customer_id
LEFT JOIN curated.Dim_Geo g ON s.customer_region = g.customer_region AND s.is_international = g.is_international
LEFT JOIN curated.Dim_Merchant m ON s.merchant_id = m.merchant_id
LEFT JOIN curated.Dim_Device d ON s.device_id = d.device_id
LEFT JOIN curated.Dim_Channel ch ON s.channel = ch.channel AND s.payment_method = ch.payment_method
LEFT JOIN curated.Dim_Time t ON s.transaction_datetime = t.transaction_datetime;
GO
--Validation
SELECT COUNT(*) AS clean_count FROM curated.Clean_Transactions;
SELECT COUNT(*) AS fact_count FROM marts.Fact_Transactions;
--Null key check
SELECT
  SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
  SUM(CASE WHEN geo_key IS NULL THEN 1 ELSE 0 END) AS null_geo_key,
  SUM(CASE WHEN merchant_key IS NULL THEN 1 ELSE 0 END) AS null_merchant_key,
  SUM(CASE WHEN device_key IS NULL THEN 1 ELSE 0 END) AS null_device_key,
  SUM(CASE WHEN channel_key IS NULL THEN 1 ELSE 0 END) AS null_channel_key,
  SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END) AS null_time_key
FROM marts.Fact_Transactions;
