-- Inserts new, cleaned records from the raw table into the cleaned table.
-- It only inserts records that have been cleaned but not yet inserted.
INSERT INTO consumer_complaints_cleaned (
    date_received, product, sub_product, issue, sub_issue, consumer_complaint_narrative, company_public_response,
    company, state_code, zip_code, tags, consumer_consent_provided, submitted_via, date_sent_to_company,
    company_response_to_consumer, timely_response, consumer_disputed, complaint_id, ingestion_date, source_file_name
)
SELECT
    date_received, product, sub_product, issue, sub_issue, consumer_complaint_narrative, company_public_response,
    company, state_code, zip_code, tags, consumer_consent_provided, submitted_via, date_sent_to_company,
    company_response_to_consumer, timely_response, consumer_disputed, complaint_id, ingestion_date, source_file_name
FROM
    consumer_complaints_staging s
WHERE
    s.cleaned_timestamp IS NOT NULL
    AND s.complaint_id NOT IN (SELECT complaint_id FROM consumer_complaints_cleaned)
{limit_clause};