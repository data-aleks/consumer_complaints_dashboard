-- This script ONLY populates the fact table and updates the source table's timestamp.
-- Populates the fact table for a specific batch of records.
-- This script is designed to be executed by a parallel worker. It selects its own
-- batch of records to process based on the worker's assigned ID range and the last
-- ID processed in the previous batch.

-- Step 1: Create a small, batch-specific temporary table (`temp_modeling_batch`).
-- This table holds the `complaint_id`s for the current batch. It is created by selecting
-- records from the main tables that fall within the worker's partition (`:start_id` to `:end_id`),
-- are greater than the last processed ID (`:last_id`), and have not yet been modeled.
DROP TEMPORARY TABLE IF EXISTS temp_modeling_batch;
CREATE TEMPORARY TABLE temp_modeling_batch (complaint_id INT PRIMARY KEY) AS
SELECT c.complaint_id
FROM {queue_table} q
JOIN consumer_complaints_cleaned c ON q.complaint_id = c.complaint_id
WHERE c.complaint_id BETWEEN :start_id AND :end_id
  AND c.complaint_id > :last_id
ORDER BY c.complaint_id -- The ORDER BY and LIMIT create a predictable, sequential batch
LIMIT :limit;

-- Step 2: Create a second temporary table that pre-joins all dimension keys.
-- This is the key performance optimization. It resolves all joins first, making the final INSERT much simpler and faster.
DROP TEMPORARY TABLE IF EXISTS temp_fact_staging;
CREATE TEMPORARY TABLE temp_fact_staging AS
SELECT
    c.complaint_id,
    d_received.date_key AS date_received_key,
    d_sent.date_key AS date_sent_key,
    p.product_key,
    sp.sub_product_key,
    i.issue_key,
    si.sub_issue_key,
    co.company_key,
    s.state_key,
    dz.zip_code_key,
    sub.origin_key,
    dcn.consent_key,
    dpr.response_key AS public_response_key,
    cr.response_key AS company_response_key,
    dt.tag_key,
    cd.disputed_key,
    c.timely_response,
    c.consumer_complaint_narrative
FROM
    temp_modeling_batch b
JOIN
    consumer_complaints_cleaned c ON b.complaint_id = c.complaint_id
LEFT JOIN dim_date d_received ON c.date_received = d_received.full_date
LEFT JOIN dim_date d_sent ON c.date_sent_to_company = d_sent.full_date
LEFT JOIN dim_product p ON c.product_standardized = p.product_name
LEFT JOIN dim_sub_product sp ON c.sub_product_standardized = sp.sub_product_name
LEFT JOIN dim_issue i ON c.issue_standardized = i.issue_name
LEFT JOIN dim_sub_issue si ON c.sub_issue_standardized = si.sub_issue_name
LEFT JOIN dim_company co ON c.company = co.company_name
LEFT JOIN dim_company_response cr ON c.company_response_to_consumer_standardized = cr.response_description
LEFT JOIN dim_state s ON c.state_code = s.state_code
LEFT JOIN dim_zip_code dz ON c.zip_code = dz.zip_code
LEFT JOIN dim_origin sub ON c.submitted_via = sub.origin_method
LEFT JOIN dim_consent dcn ON c.consumer_consent_provided_standardized = dcn.consent_status
LEFT JOIN dim_public_response dpr ON c.company_public_response_standardized = dpr.response_text
LEFT JOIN dim_disputed cd ON c.consumer_disputed_standardized = cd.disputed_status
LEFT JOIN dim_tag dt ON c.tags_standardized = dt.tag_name;

-- Step 3: Insert into the final fact table from the pre-joined staging table.
-- This operation is now a simple, fast, direct insert.
-- Using INSERT IGNORE makes this operation idempotent, preventing errors from duplicate complaint_ids on reruns.
-- The worker will insert into a unique staging table to avoid deadlocks. The main process will consolidate.
INSERT IGNORE INTO {fact_staging_table} (
    complaint_id, date_received_key, date_sent_key, product_key, sub_product_key, 
    issue_key, sub_issue_key, company_key, state_key, zip_code_key, origin_key, 
    consent_key, public_response_key, company_response_key, tag_key, disputed_key,
    timely_response, consumer_complaint_narrative
)
SELECT * FROM temp_fact_staging;