-- Standardizes 'Company response to consumer' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET company_response_to_consumer = NULL
WHERE company_response_to_consumer = '' {incremental_clause} {limit_clause};