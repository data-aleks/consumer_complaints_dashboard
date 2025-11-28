-- Inserts new, cleaned records from the raw table into the cleaned table.
-- Inserts new, cleaned records from the staging table into the cleaned table.
-- It only inserts records that have been cleaned but not yet inserted.
-- It also performs necessary type conversions during insertion.
INSERT IGNORE INTO consumer_complaints_cleaned (
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
    s.date_received,
    s.date_sent_to_company,
    s.timely_response, -- This value is already standardized to '1' or '0' by the cleaning script.
    s.product, s.product_standardized, s.sub_product, s.sub_product_standardized, s.issue, s.issue_standardized, s.sub_issue, s.sub_issue_standardized,
    s.consumer_complaint_narrative, s.company_public_response, s.company_public_response_standardized, s.company, s.state_code, s.zip_code,
    s.tags, s.tags_standardized, s.consumer_consent_provided, s.consumer_consent_provided_standardized, s.submitted_via,
    s.company_response_to_consumer, s.company_response_to_consumer_standardized, s.consumer_disputed, s.consumer_disputed_standardized,
    s.complaint_id, s.ingestion_date, s.source_file_name
FROM {staging_table} s