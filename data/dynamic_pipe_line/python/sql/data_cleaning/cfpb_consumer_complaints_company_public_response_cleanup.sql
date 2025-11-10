-- Standardizes 'Company public response' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET company_public_response = NULL
WHERE company_public_response = '' {incremental_clause} {limit_clause};