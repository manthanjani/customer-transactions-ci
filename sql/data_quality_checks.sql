
USE Myproject

-- Create the SQL file for data quality checks
-- Check 1: Missing customers in transactions
SELECT COUNT(*) AS MissingCustomerID
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check 2: Negative or zero transaction amounts
SELECT COUNT(*) AS InvalidAmounts
FROM transactions
WHERE amount <= 0;

-- Check 3: Nulls in key customer columns
SELECT COUNT(*) AS NullFields
FROM customers
WHERE name IS NULL OR region IS NULL OR income IS NULL;
