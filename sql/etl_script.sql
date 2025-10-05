-- create schemas if they don't exist (WORKSPACE SETUP)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw') EXEC('CREATE SCHEMA raw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated') EXEC('CREATE SCHEMA curated');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics') EXEC('CREATE SCHEMA analytics');

-- 1A. basic row counts
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM dbo.customers;
SELECT 'transactions' AS tbl, COUNT(*) AS rows FROM dbo.transactions;

-- 1B. null counts in customers
SELECT
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN LTRIM(RTRIM(name)) = '' OR name IS NULL THEN 1 ELSE 0 END) AS null_name,
  SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS null_age,
  SUM(CASE WHEN income IS NULL THEN 1 ELSE 0 END) AS null_income,
  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
  SUM(CASE WHEN account_type IS NULL THEN 1 ELSE 0 END) AS null_account_type
FROM dbo.customers;

-- 1C. null counts in transactions
SELECT
  SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN [date] IS NULL THEN 1 ELSE 0 END) AS null_date,
  SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
  SUM(CASE WHEN transaction_type IS NULL THEN 1 ELSE 0 END) AS null_transaction_type,
  SUM(CASE WHEN merchant_category IS NULL THEN 1 ELSE 0 END) AS null_merchant_category,
  SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country,
  SUM(CASE WHEN is_fraud IS NULL THEN 1 ELSE 0 END) AS null_is_fraud
FROM dbo.transactions;

-- 1D. distinct merchant categories sample
SELECT TOP 50 merchant_category, COUNT(*) cnt
FROM dbo.transactions
GROUP BY merchant_category
ORDER BY cnt DESC;

--2) Correct Data Types

-- 2A. drop if they exist (for re-runs)
IF OBJECT_ID('staging.customers_stg','U') IS NOT NULL DROP TABLE staging.customers_stg;
IF OBJECT_ID('staging.transactions_stg','U') IS NOT NULL DROP TABLE staging.transactions_stg;

-- 2B. create staging.customers_stg
CREATE TABLE staging.customers_stg (
  customer_id UNIQUEIDENTIFIER PRIMARY KEY,
  name NVARCHAR(200),
  age INT,
  income DECIMAL(12,2),
  region CHAR(2),
  account_type NVARCHAR(50),
  raw_name_original NVARCHAR(200) NULL -- optional for traceability
);

-- 2C. create staging.transactions_stg
CREATE TABLE staging.transactions_stg (
  transaction_id UNIQUEIDENTIFIER PRIMARY KEY,
  customer_id UNIQUEIDENTIFIER NULL,
  transaction_date DATETIME2 NULL,
  amount DECIMAL(12,2) NULL,
  transaction_type NVARCHAR(50),
  merchant_category NVARCHAR(100),
  country NVARCHAR(100),
  is_fraud BIT DEFAULT(0),
  raw_date_original NVARCHAR(100) NULL -- keep original string for debugging
);

--) 3) Load & clean into staging (transform)

-- 3A. customers -> staging
INSERT INTO staging.customers_stg (customer_id, name, age, income, region, account_type, raw_name_original)
SELECT
  TRY_CONVERT(uniqueidentifier, LTRIM(RTRIM(customer_id))) AS customer_id,
  LTRIM(RTRIM(name)) AS name,
  TRY_CONVERT(INT, age) AS age,
  TRY_CONVERT(DECIMAL(12,2), income) AS income,
  UPPER(LEFT(LTRIM(RTRIM(region)), 2)) AS region,
  CASE
    WHEN LOWER(LTRIM(RTRIM(account_type))) IN ('chequing','checking') THEN 'Chequing'
    WHEN LOWER(LTRIM(RTRIM(account_type))) = 'savings' THEN 'Savings'
    WHEN LOWER(LTRIM(RTRIM(account_type))) = 'credit' THEN 'Credit'
    ELSE LTRIM(RTRIM(account_type))
  END AS account_type,
  name AS raw_name_original
FROM dbo.customers
WHERE TRY_CONVERT(uniqueidentifier, LTRIM(RTRIM(customer_id))) IS NOT NULL; -- keep valid GUIDs

-- 3B. transactions -> staging
INSERT INTO staging.transactions_stg
(transaction_id, customer_id, transaction_date, amount, transaction_type, merchant_category, country, is_fraud, raw_date_original)
SELECT
  TRY_CONVERT(uniqueidentifier, LTRIM(RTRIM(transaction_id))) AS transaction_id,
  TRY_CONVERT(uniqueidentifier, LTRIM(RTRIM(customer_id))) AS customer_id,
  TRY_CONVERT(datetime2, LTRIM(RTRIM([date]))) AS transaction_date, -- format 'YYYY-MM-DD HH:MM' should parse
  TRY_CONVERT(DECIMAL(12,2), amount) AS amount,
  LTRIM(RTRIM(transaction_type)) AS transaction_type,
  LTRIM(RTRIM(merchant_category)) AS merchant_category,
  LTRIM(RTRIM(country)) AS country,
  CASE WHEN TRY_CONVERT(INT, is_fraud) = 1 THEN 1 ELSE 0 END AS is_fraud,
  [date] AS raw_date_original
FROM dbo.transactions
WHERE TRY_CONVERT(uniqueidentifier, LTRIM(RTRIM(transaction_id))) IS NOT NULL; -- only valid transaction ids


-- 4) Find Bad Missing Rows and Fix or Log

-- 4A. transactions with invalid/missing parsed date or amount
SELECT * FROM dbo.transactions
WHERE TRY_CONVERT(datetime2, LTRIM(RTRIM([date]))) IS NULL
   OR TRY_CONVERT(DECIMAL(12,2), amount) IS NULL;

-- 4B. transactions in staging that have no matching customer in staging
SELECT t.* 
FROM staging.transactions_stg t
LEFT JOIN staging.customers_stg c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Option: create placeholder customers for orphan transactions (if acceptable)
INSERT INTO staging.customers_stg (customer_id, name)
SELECT DISTINCT t.customer_id, 'UNKNOWN' 
FROM staging.transactions_stg t
LEFT JOIN staging.customers_stg c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL AND t.customer_id IS NOT NULL;

-- Deduplicate

-- Example: remove duplicate transactions if any (keep first)
;WITH cte AS (
  SELECT transaction_id,
         ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_date) AS rn
  FROM staging.transactions_stg
)
DELETE t
FROM staging.transactions_stg t
JOIN cte ON t.transaction_id = cte.transaction_id
WHERE cte.rn > 1;

-- For customers (if duplicates by customer_id):
;WITH cte2 AS (
  SELECT customer_id,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY (SELECT 0)) AS rn
  FROM staging.customers_stg
)
DELETE c
FROM staging.customers_stg c
JOIN cte2 ON c.customer_id = cte2.customer_id
WHERE cte2.rn > 1;

--6) Create curated tables (dimension & fact)

-- 6A. curated dim_customers
IF OBJECT_ID('curated.dim_customers','U') IS NOT NULL DROP TABLE curated.dim_customers;
SELECT customer_id, name, age, income, region, account_type
INTO curated.dim_customers
FROM staging.customers_stg;

-- 6B. curated fact_transactions
IF OBJECT_ID('curated.fact_transactions','U') IS NOT NULL DROP TABLE curated.fact_transactions;
SELECT transaction_id, customer_id, transaction_date, amount, transaction_type, merchant_category, country, is_fraud
INTO curated.fact_transactions
FROM staging.transactions_stg;

-- 7) Analytics view — customer metrics (aggregates)
-- 7A. create or replace view
IF OBJECT_ID('analytics.customer_metrics','V') IS NOT NULL DROP VIEW analytics.customer_metrics;
GO

CREATE VIEW analytics.customer_metrics AS
SELECT
  c.customer_id,
  c.name,
  c.region,
  COUNT(f.transaction_id) AS total_transactions,
  SUM(f.amount) AS total_spent,
  AVG(f.amount) AS avg_transaction,
  MIN(f.transaction_date) AS first_transaction_date,
  MAX(f.transaction_date) AS last_transaction_date,
  SUM(CASE WHEN f.is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
  CASE WHEN COUNT(f.transaction_id) = 0 THEN 0.0
       ELSE CAST(SUM(CASE WHEN f.is_fraud = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(f.transaction_id)
  END AS fraud_rate,
  COUNT(DISTINCT f.merchant_category) AS distinct_merchant_categories
FROM curated.dim_customers c
LEFT JOIN curated.fact_transactions f ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.name, c.region;
GO

--8) Data quality checks you can automate These are the checks you’ll later run in CI (GitHub Actions). They return rows that violate the rule.
-- 8A. negative amounts (should be none)
SELECT * FROM curated.fact_transactions WHERE amount < 0;

-- 8B. future dates (transactions dated after server time)
SELECT * FROM curated.fact_transactions WHERE transaction_date > GETDATE();

-- 8C. income <= 0
SELECT * FROM curated.dim_customers WHERE income <= 0 OR income IS NULL;

-- 8D. very odd ages (< 0 or > 120)
SELECT * FROM curated.dim_customers WHERE age < 0 OR age > 120 OR age IS NULL;

-- 8E. check for duplicate transaction ids in curated table (sanity)
SELECT transaction_id, COUNT(*) cnt FROM curated.fact_transactions GROUP BY transaction_id HAVING COUNT(*) > 1;


-- 9) Incremental loads & idempotency (how to run ETL repeatedly)
-- 9A. MERGE customers (upsert)
MERGE INTO curated.dim_customers AS target
USING (SELECT * FROM staging.customers_stg) AS src
ON target.customer_id = src.customer_id
WHEN MATCHED THEN
  UPDATE SET name = src.name, age = src.age, income = src.income, region = src.region, account_type = src.account_type
WHEN NOT MATCHED BY TARGET THEN
  INSERT (customer_id, name, age, income, region, account_type)
  VALUES (src.customer_id, src.name, src.age, src.income, src.region, src.account_type);

-- 9B. MERGE transactions (insert new transactions, update if necessary)
MERGE INTO curated.fact_transactions AS target
USING (SELECT * FROM staging.transactions_stg) AS src
ON target.transaction_id = src.transaction_id
WHEN MATCHED THEN
  UPDATE SET transaction_date = src.transaction_date, amount = src.amount, transaction_type = src.transaction_type,
             merchant_category = src.merchant_category, country = src.country, is_fraud = src.is_fraud
WHEN NOT MATCHED BY TARGET THEN
  INSERT (transaction_id, customer_id, transaction_date, amount, transaction_type, merchant_category, country, is_fraud)
  VALUES (src.transaction_id, src.customer_id, src.transaction_date, src.amount, src.transaction_type, src.merchant_category, src.country, src.is_fraud);

  --10) Export to CSV (so you can upload to AWS S3)

 