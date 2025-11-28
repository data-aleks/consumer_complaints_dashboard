-- Step 1: Truncate the staging table to ensure it's a fresh workspace for this run.
-- Insert a specific batch of records, identified by a unique run_id, from the raw table into the staging table.
-- The calling Python script is responsible for TRUNCATE and for marking the rows with the run_id.
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
    source_file_name
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
    source_file_name
FROM consumer_complaints_raw -- Select rows locked for this specific run
WHERE staging_run_id = :run_id -- Use the correct named parameter syntax