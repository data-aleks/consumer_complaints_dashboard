-- Standardizes 'Tags' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET tags = NULL
WHERE tags = '' {incremental_clause} {limit_clause};