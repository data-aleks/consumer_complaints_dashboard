-- Standardizes 'Company public response' by setting empty strings or NULLs to 'None'.
UPDATE consumer_complaints_raw
SET company_public_response = 'None'
WHERE (company_public_response = '' OR company_public_response IS NULL) {incremental_clause} {limit_clause};