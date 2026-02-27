--STEP 0 — Ensure we are in the correct database

SELECT DB_NAME() AS current_db;
GO

--step 1
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated') EXEC('CREATE SCHEMA curated');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'marts')   EXEC('CREATE SCHEMA marts');
GO

--STEP 2 — Move raw staging table into staging schema
--(Enterprise convention: raw stays in staging.*)
IF OBJECT_ID('dbo.stg_transactions','U') IS NOT NULL
AND  OBJECT_ID('staging.stg_transactions','U') IS NULL
BEGIN
    ALTER SCHEMA staging TRANSFER dbo.stg_transactions;
END
GO

--STEP 3 — Verify staging table exists and has data
SELECT COUNT(*) AS staging_count FROM staging.stg_transactions;
SELECT TOP 5 * FROM staging.stg_transactions;
GO


--STEP 4 — Drop existing warehouse tables (clean rebuild)
--Reason: during development you may rerun scripts; dropping ensures a clean run.

IF OBJECT_ID('marts.Fact_Transactions','U') IS NOT NULL DROP TABLE marts.Fact_Transactions;

IF OBJECT_ID('curated.Dim_Time','U') IS NOT NULL DROP TABLE curated.Dim_Time;
IF OBJECT_ID('curated.Dim_Channel','U') IS NOT NULL DROP TABLE curated.Dim_Channel;
IF OBJECT_ID('curated.Dim_Device','U') IS NOT NULL DROP TABLE curated.Dim_Device;
IF OBJECT_ID('curated.Dim_Merchant','U') IS NOT NULL DROP TABLE curated.Dim_Merchant;
IF OBJECT_ID('curated.Dim_Geo','U') IS NOT NULL DROP TABLE curated.Dim_Geo;
IF OBJECT_ID('curated.Dim_Customer','U') IS NOT NULL DROP TABLE curated.Dim_Customer;
GO


--STEP 5 — Create dimension tables (curated layer)
--Governance fields:
--ingestion_dt: load timestamp
--source_system: lineage (e.g., FraudCSV)
--load_batch_id: batch/run id


--Dim_Customer: customer attributes repeated across many transactions (store once) 
CREATE TABLE curated.Dim_Customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_age INT NULL,
    account_age_days INT NULL,
    previous_fraud_flag INT NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);

--Dim_Geo: geography + international indicator (store once) 
CREATE TABLE curated.Dim_Geo (
    geo_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_region VARCHAR(100) NOT NULL,
    is_international INT NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);

--Dim_Merchant: merchant id + category (store once) 
CREATE TABLE curated.Dim_Merchant (
    merchant_key INT IDENTITY(1,1) PRIMARY KEY,
    merchant_id VARCHAR(50) NOT NULL,
    merchant_category VARCHAR(100) NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);

-- Dim_Device: device + network attributes (important fraud drivers) 
CREATE TABLE curated.Dim_Device (
    device_key INT IDENTITY(1,1) PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    device_type VARCHAR(50) NULL,
    ip_address VARCHAR(50) NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);

--Dim_Channel: channel + payment method (reporting dimensions) 
CREATE TABLE curated.Dim_Channel (
    channel_key INT IDENTITY(1,1) PRIMARY KEY,
    channel VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);

--Dim_Time: time slicing for dashboards (daily/hourly trends) 
CREATE TABLE curated.Dim_Time (
    time_key INT IDENTITY(1,1) PRIMARY KEY,
    transaction_datetime DATETIME NOT NULL,
    transaction_date DATE NULL,
    transaction_hour INT NULL,
    transaction_month INT NULL,
    transaction_year INT NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);
GO


--STEP 6 — Create fact table (marts layer)
-- stores measures + surrogate keys to all dimensions
--record_hash provides auditability & duplicate/change detection

CREATE TABLE marts.Fact_Transactions (
    transaction_key INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    customer_key INT NULL,
    geo_key INT NULL,
    merchant_key INT NULL,
    device_key INT NULL,
    channel_key INT NULL,
    time_key INT NULL,
    transaction_amount DECIMAL(18,2) NULL,
    fraud_label INT NULL,
    record_hash VARCHAR(100) NULL,
    ingestion_dt DATETIME NOT NULL DEFAULT GETDATE(),
    source_system VARCHAR(50) NULL,
    load_batch_id INT NULL
);
GO


--STEP 7 — Load dimensions from staging (distinct entities)
--IMPORTANT: Use DISTINCT to load unique business keys only.

--Load Dim_Customer 
INSERT INTO curated.Dim_Customer (customer_id, customer_age, account_age_days, previous_fraud_flag, source_system, load_batch_id)
SELECT DISTINCT
    customer_id,
    customer_age,
    account_age_days,
    previous_fraud_flag,
    'FraudCSV',
    1
FROM staging.stg_transactions;

-- Load Dim_Geo
INSERT INTO curated.Dim_Geo (customer_region, is_international, source_system, load_batch_id)
SELECT DISTINCT
    customer_region,
    is_international,
    'FraudCSV',
    1
FROM staging.stg_transactions;

--Load Dim_Merchant 
INSERT INTO curated.Dim_Merchant (merchant_id, merchant_category, source_system, load_batch_id)
SELECT DISTINCT
    merchant_id,
    merchant_category,
    'FraudCSV',
    1
FROM staging.stg_transactions;

-- Load Dim_Device 
INSERT INTO curated.Dim_Device (device_id, device_type, ip_address, source_system, load_batch_id)
SELECT DISTINCT
    device_id,
    device_type,
    ip_address,
    'FraudCSV',
    1
FROM staging.stg_transactions;

--Load Dim_Channel 
INSERT INTO curated.Dim_Channel (channel, payment_method, source_system, load_batch_id)
SELECT DISTINCT
    channel,
    payment_method,
    'FraudCSV',
    1
FROM staging.stg_transactions;

-- Load Dim_Time (convert timestamp text safely; keep only convertible timestamps) 
INSERT INTO curated.Dim_Time (transaction_datetime, transaction_date, transaction_hour, transaction_month, transaction_year, source_system, load_batch_id)
SELECT DISTINCT
    dt.transaction_datetime,
    CAST(dt.transaction_datetime AS DATE),
    DATEPART(HOUR, dt.transaction_datetime),
    DATEPART(MONTH, dt.transaction_datetime),
    DATEPART(YEAR, dt.transaction_datetime),
    'FraudCSV',
    1
FROM (
    SELECT TRY_CONVERT(datetime, transaction_timestamp, 120) AS transaction_datetime
    FROM staging.stg_transactions
) dt
WHERE dt.transaction_datetime IS NOT NULL;
GO


--STEP 8 — Add uniqueness protection (prevents duplicate dimensions on reruns)
--This avoids row multiplication during fact joins.
CREATE UNIQUE INDEX UX_DimCustomer_customer_id ON curated.Dim_Customer(customer_id);
CREATE UNIQUE INDEX UX_DimGeo_region_international ON curated.Dim_Geo(customer_region, is_international);
CREATE UNIQUE INDEX UX_DimMerchant_merchant_id ON curated.Dim_Merchant(merchant_id);
CREATE UNIQUE INDEX UX_DimDevice_device_id ON curated.Dim_Device(device_id);
CREATE UNIQUE INDEX UX_DimChannel_channel_payment ON curated.Dim_Channel(channel, payment_method);
CREATE UNIQUE INDEX UX_DimTime_transaction_datetime ON curated.Dim_Time(transaction_datetime);
GO

--STEP 9 — Load fact table by mapping staging rows to surrogate keys
-- LEFT JOIN keeps completeness (does not drop transactions)
-- record_hash is an audit fingerprint for lineage/change detection

INSERT INTO marts.Fact_Transactions (
    transaction_id,
    customer_key,
    geo_key,
    merchant_key,
    device_key,
    channel_key,
    time_key,
    transaction_amount,
    fraud_label,
    record_hash,
    source_system,
    load_batch_id
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
            CONCAT(
                s.transaction_id,
                s.customer_id,
                s.merchant_id,
                s.transaction_amount,
                s.transaction_timestamp
            )
        ), 2
    ) AS record_hash,
    'FraudCSV',
    1
FROM staging.stg_transactions s
LEFT JOIN curated.Dim_Customer c
    ON s.customer_id = c.customer_id
LEFT JOIN curated.Dim_Geo g
    ON s.customer_region = g.customer_region
   AND s.is_international = g.is_international
LEFT JOIN curated.Dim_Merchant m
    ON s.merchant_id = m.merchant_id
LEFT JOIN curated.Dim_Device d
    ON s.device_id = d.device_id
LEFT JOIN curated.Dim_Channel ch
    ON s.channel = ch.channel
   AND s.payment_method = ch.payment_method
LEFT JOIN curated.Dim_Time t
    ON TRY_CONVERT(datetime, s.transaction_timestamp, 120) = t.transaction_datetime;
GO

--STEP 10 — Validations (must pass before Phase 3)


--Row counts 
SELECT COUNT(*) AS staging_count FROM staging.stg_transactions;
SELECT COUNT(*) AS dim_customer FROM curated.Dim_Customer;
SELECT COUNT(*) AS dim_geo FROM curated.Dim_Geo;
SELECT COUNT(*) AS dim_merchant FROM curated.Dim_Merchant;
SELECT COUNT(*) AS dim_device FROM curated.Dim_Device;
SELECT COUNT(*) AS dim_channel FROM curated.Dim_Channel;
SELECT COUNT(*) AS dim_time FROM curated.Dim_Time;
SELECT COUNT(*) AS fact_count FROM marts.Fact_Transactions;

--Key completeness check (should be 0s ideally; if not, we add "Unknown" dimension rows later) 
SELECT
  SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
  SUM(CASE WHEN geo_key IS NULL THEN 1 ELSE 0 END) AS null_geo_key,
  SUM(CASE WHEN merchant_key IS NULL THEN 1 ELSE 0 END) AS null_merchant_key,
  SUM(CASE WHEN device_key IS NULL THEN 1 ELSE 0 END) AS null_device_key,
  SUM(CASE WHEN channel_key IS NULL THEN 1 ELSE 0 END) AS null_channel_key,
  SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END) AS null_time_key
FROM marts.Fact_Transactions;
GO

--Validating before moving to the next phase
SELECT COUNT(*) AS staging_count FROM staging.stg_transactions;
SELECT COUNT(*) AS fact_count FROM marts.Fact_Transactions;