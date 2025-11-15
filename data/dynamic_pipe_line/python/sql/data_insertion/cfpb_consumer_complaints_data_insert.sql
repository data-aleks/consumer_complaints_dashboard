-- Inserts new, cleaned records from the raw table into the cleaned table.
-- It only inserts records that have been cleaned but not yet inserted.
INSERT INTO consumer_complaints_cleaned (
    date_received, product, sub_product, issue, sub_issue, consumer_complaint_narrative, company_public_response,
    company, state_code, zip_code, tags, consumer_consent_provided, submitted_via, date_sent_to_company,
    company_response_to_consumer, timely_response, consumer_disputed, complaint_id, ingestion_date, source_file_name
)
SELECT
    s.date_received, s.product, s.sub_product, s.issue, s.sub_issue, s.consumer_complaint_narrative, s.company_public_response,
    s.company, s.state_code, s.zip_code, s.tags, s.consumer_consent_provided, s.submitted_via, s.date_sent_to_company,
    s.company_response_to_consumer, s.timely_response, s.consumer_disputed, s.complaint_id, s.ingestion_date, s.source_file_name
FROM
    consumer_complaints_staging s -- Records that have been cleaned
LEFT JOIN
    consumer_complaints_cleaned c ON s.complaint_id = c.complaint_id -- Check if they already exist in the cleaned table
WHERE
    s.cleaned_timestamp IS NOT NULL
    AND c.complaint_id IS NULL -- This condition finds records from staging that are NOT in cleaned
{limit_clause};