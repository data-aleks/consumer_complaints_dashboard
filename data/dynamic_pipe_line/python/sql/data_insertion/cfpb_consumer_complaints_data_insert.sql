-- CFPB Consumer Complaints Schema Cleanup
-- Appends new cleaned records from raw table into cleaned table
-- Avoids dropping or overwriting existing data

INSERT INTO consumer_complaints_cleaned (
  complaint_id,
  company_name,
  company_public_response,
  company_response,
  consumer_narrative,
  consumer_consent,
  consumer_disputed,
  date_received,
  date_sent_to_company,
  issue,
  product,
  state_code,
  sub_issue,
  sub_product,
  submitted_via,
  tags,
  timely_response,
  zip_code,
  ingestion_date,
  source_file_name
)
SELECT
  r.`Complaint ID`,
  r.`Company`,
  r.`Company public response`,
  r.`Company response to consumer`,
  r.`Consumer complaint narrative`,
  r.`Consumer consent provided?`,
  r.`Consumer disputed?`,
  r.`Date received`,
  r.`Date sent to company`,
  r.`Issue`,
  r.`Product`,
  r.`State`,
  r.`Sub-issue`,
  r.`Sub-product`,
  r.`Submitted via`,
  r.`Tags`,
  r.`Timely response?`,
  r.`ZIP code`,
  r.ingestion_date,
  r.source_file_name
FROM consumer_complaints_raw r
WHERE NOT EXISTS (
  SELECT 1
  FROM consumer_complaints_cleaned c
  WHERE c.complaint_id = r.`Complaint ID`
);