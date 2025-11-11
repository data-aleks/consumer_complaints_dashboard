-- Standardizes 'Tags' by setting empty strings or NULLs to 'None'.
UPDATE consumer_complaints_raw
SET tags = 'None'
WHERE (tags IS NULL OR TRIM(tags) = '') {incremental_clause} {limit_clause};