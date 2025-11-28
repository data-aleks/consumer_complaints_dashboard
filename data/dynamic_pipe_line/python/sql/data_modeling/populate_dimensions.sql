-- This script ONLY populates dimension tables from a given temporary table of records.
-- This script populates dimension tables from a given temporary table of records.
-- It is designed to be run once in a single-threaded step before parallel fact table modeling.

-- Step 1: Create a temporary table with all distinct dimension values for this run.
-- This is the key optimization: we scan the large 'consumer_complaints_cleaned' table only ONCE.
-- The resulting temp table is very small, containing only unique dimension values.
-- Step 1: Create a temporary table with all distinct dimension values for this run.
-- This is the key optimization: we scan the large 'consumer_complaints_cleaned' table only ONCE.
-- The resulting temp table is very small, containing only unique dimension values.
DROP TEMPORARY TABLE IF EXISTS temp_distinct_dimensions;
CREATE TEMPORARY TABLE temp_distinct_dimensions AS
SELECT DISTINCT
    c.date_received,
    c.date_sent_to_company,
    c.product_standardized,
    c.sub_product_standardized,
    c.issue_standardized,
    c.sub_issue_standardized,
    c.company,
    c.company_response_to_consumer_standardized,
    c.state_code,
    c.zip_code,
    c.submitted_via,
    c.consumer_consent_provided_standardized,
    c.company_public_response_standardized,
    c.consumer_disputed_standardized,
    c.tags_standardized
FROM consumer_complaints_cleaned c
JOIN {queue_table} q ON c.complaint_id = q.complaint_id;

-- Add indexes to the temporary table to dramatically speed up the following DISTINCT selects.
CREATE INDEX idx_temp_state ON temp_distinct_dimensions (state_code);
CREATE INDEX idx_temp_zip ON temp_distinct_dimensions (zip_code);
-- Add more indexes as needed for other frequently queried dimension columns.

-- Step 2: Populate date dimension from the new, small temporary table.

-- First, materialize the distinct dates into a new temporary table.
-- This is necessary to avoid MySQL's "Can't reopen table" error when selecting from a temp table in a subquery of an INSERT.
CREATE TEMPORARY TABLE temp_distinct_dates (full_date DATE PRIMARY KEY);

-- Insert distinct received dates.
INSERT IGNORE INTO temp_distinct_dates (full_date)
SELECT DISTINCT date_received FROM temp_distinct_dimensions WHERE date_received IS NOT NULL;

-- Insert distinct sent dates. `INSERT IGNORE` handles any duplicates from the previous insert.
INSERT IGNORE INTO temp_distinct_dates (full_date)
SELECT DISTINCT date_sent_to_company FROM temp_distinct_dimensions WHERE date_sent_to_company IS NOT NULL;

-- Now, populate dim_date from the simple, materialized temporary table, which avoids the error.
INSERT IGNORE INTO dim_date (
    full_date, `year`, `month`, `day`, `quarter`, day_of_week_number,
    month_name, day_name, week_of_year, day_of_year, is_weekend,
    is_month_end, is_quarter_end, is_year_end,
    fiscal_year, fiscal_quarter, fiscal_month
)
SELECT DISTINCT
    full_date,
    YEAR(full_date), MONTH(full_date), DAY(full_date), QUARTER(full_date),
    DAYOFWEEK(full_date), DATE_FORMAT(full_date, '%M'), DATE_FORMAT(full_date, '%W'),
    WEEKOFYEAR(full_date), DAYOFYEAR(full_date), (DAYOFWEEK(full_date) IN (1, 7)),
    (LAST_DAY(full_date) = full_date),
    (LAST_DAY(DATE_ADD(MAKEDATE(YEAR(full_date), 1), INTERVAL QUARTER(full_date) * 3 - 1 MONTH)) = full_date),
    (MONTH(full_date) = 12 AND DAY(full_date) = 31),
    IF(MONTH(full_date) >= 10, YEAR(full_date) + 1, YEAR(full_date)),
    IF(MONTH(full_date) >= 10, QUARTER(full_date) - 3, QUARTER(full_date) + 1),
    MOD(MONTH(full_date) - 10 + 12, 12) + 1
FROM temp_distinct_dates;

DROP TEMPORARY TABLE temp_distinct_dates;

-- Step 3: Populate all other dimension tables from the small temporary table.
-- These operations are now extremely fast as they read from a small, pre-aggregated source.
INSERT IGNORE INTO dim_company (company_name) SELECT DISTINCT company FROM temp_distinct_dimensions WHERE company IS NOT NULL;
INSERT IGNORE INTO dim_consent (consent_status) SELECT DISTINCT consumer_consent_provided_standardized FROM temp_distinct_dimensions WHERE consumer_consent_provided_standardized IS NOT NULL;
INSERT IGNORE INTO dim_disputed (disputed_status) SELECT DISTINCT consumer_disputed_standardized FROM temp_distinct_dimensions WHERE consumer_disputed_standardized IS NOT NULL;
INSERT IGNORE INTO dim_issue (issue_name) SELECT DISTINCT issue_standardized FROM temp_distinct_dimensions WHERE issue_standardized IS NOT NULL;
INSERT IGNORE INTO dim_origin (origin_method) SELECT DISTINCT submitted_via FROM temp_distinct_dimensions WHERE submitted_via IS NOT NULL;
INSERT IGNORE INTO dim_product (product_name) SELECT DISTINCT product_standardized FROM temp_distinct_dimensions WHERE product_standardized IS NOT NULL;
INSERT IGNORE INTO dim_public_response (response_text) SELECT DISTINCT company_public_response_standardized FROM temp_distinct_dimensions WHERE company_public_response_standardized IS NOT NULL;
INSERT IGNORE INTO dim_company_response (response_description) SELECT DISTINCT company_response_to_consumer_standardized FROM temp_distinct_dimensions WHERE company_response_to_consumer_standardized IS NOT NULL;
INSERT IGNORE INTO dim_sub_issue (sub_issue_name) SELECT DISTINCT sub_issue_standardized FROM temp_distinct_dimensions WHERE sub_issue_standardized IS NOT NULL;
INSERT IGNORE INTO dim_sub_product (sub_product_name) SELECT DISTINCT sub_product_standardized FROM temp_distinct_dimensions WHERE sub_product_standardized IS NOT NULL;
INSERT IGNORE INTO dim_tag (tag_name) SELECT DISTINCT tags_standardized FROM temp_distinct_dimensions WHERE tags_standardized IS NOT NULL;
INSERT IGNORE INTO dim_zip_code (zip_code) SELECT DISTINCT zip_code FROM temp_distinct_dimensions WHERE zip_code IS NOT NULL;

INSERT IGNORE INTO dim_state (state_code) SELECT DISTINCT state_code FROM temp_distinct_dimensions WHERE state_code IS NOT NULL;

-- Step 4: Clean up the temporary table.
DROP TEMPORARY TABLE temp_distinct_dimensions;