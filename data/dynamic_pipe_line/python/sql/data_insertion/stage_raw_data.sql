-- Step 1: Truncate the staging table to ensure it's a fresh workspace for this run.
-- TRUNCATE is faster than DELETE for clearing an entire table.
TRUNCATE TABLE consumer_complaints_staging;

-- Step 2: Insert only the new, unprocessed records from the raw table into the staging table.
-- We identify new records by checking if `cleaned_timestamp` is NULL.
INSERT INTO consumer_complaints_staging (
    date_received,
    product,
    sub_product,
    issue,
    sub_issue,
    consumer_complaint_narrative,
    company_public_response,
    company,
    state_code,
    zip_code,
    tags,
    consumer_consent_provided,
    submitted_via,
    date_sent_to_company,
    company_response_to_consumer,
    timely_response,
    consumer_disputed,
    complaint_id,
    ingestion_date,
    source_file_name,
    cleaned_timestamp
)
SELECT -- Explicitly list columns to prevent future schema mismatch errors.
    date_received,
    product,
    sub_product,
    issue,
    sub_issue,
    consumer_complaint_narrative,
    company_public_response,
    company,
    state_code,
    zip_code,
    tags,
    consumer_consent_provided,
    submitted_via,
    date_sent_to_company,
    company_response_to_consumer,
    timely_response,
    consumer_disputed,
    complaint_id,
    ingestion_date,
    source_file_name,
    cleaned_timestamp
FROM consumer_complaints_raw
WHERE cleaned_timestamp IS NULL
{limit_clause};