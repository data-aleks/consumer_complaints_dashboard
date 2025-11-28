-- Inserts new, cleaned records from the staging table into the cleaned table.
-- It only inserts records that have been cleaned but not yet inserted.
-- It also performs necessary type conversions during insertion.
INSERT INTO consumer_complaints_cleaned (
    date_received, date_sent_to_company, timely_response,
    product, product_standardized,
    sub_product, sub_product_standardized,
    issue, issue_standardized,
    sub_issue, sub_issue_standardized,
    consumer_complaint_narrative,
    company_public_response, company_public_response_standardized,
    company, state_code, zip_code,
    tags, tags_standardized,
    consumer_consent_provided, consumer_consent_provided_standardized,
    submitted_via,
    company_response_to_consumer, company_response_to_consumer_standardized,
    consumer_disputed, consumer_disputed_standardized,
    complaint_id, ingestion_date, source_file_name
)
SELECT
    -- Use a CASE statement to safely parse dates. If the format is invalid, insert NULL instead of erroring.
    -- The REGEXP checks for a YYYY-MM-DD format.
    CASE WHEN s.date_received REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
         THEN STR_TO_DATE(s.date_received, '%%Y-%%m-%%d') 
         ELSE NULL END,
    CASE WHEN s.date_sent_to_company REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
         THEN STR_TO_DATE(s.date_sent_to_company, '%%Y-%%m-%%d') 
         ELSE NULL END,
    CASE WHEN UPPER(TRIM(s.timely_response)) = 'YES' THEN 1 WHEN UPPER(TRIM(s.timely_response)) = 'NO' THEN 0 ELSE NULL END,
    s.product, s.product_standardized, s.sub_product, s.sub_product_standardized, s.issue, s.issue_standardized, s.sub_issue, s.sub_issue_standardized,
    s.consumer_complaint_narrative, s.company_public_response, s.company_public_response_standardized, s.company, s.state_code, s.zip_code,
    s.tags, s.tags_standardized, s.consumer_consent_provided, s.consumer_consent_provided_standardized, s.submitted_via,
    s.company_response_to_consumer, s.company_response_to_consumer_standardized, s.consumer_disputed, s.consumer_disputed_standardized,
    s.complaint_id, s.ingestion_date, s.source_file_name
FROM consumer_complaints_staging s
WHERE s.cleaned_timestamp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM consumer_complaints_cleaned c WHERE c.complaint_id = s.complaint_id) {limit_clause};