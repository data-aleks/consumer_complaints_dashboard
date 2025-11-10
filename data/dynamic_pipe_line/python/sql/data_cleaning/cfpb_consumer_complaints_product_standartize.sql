-- Standardizes 'Product' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET product = NULL
WHERE product = '' {incremental_clause} {limit_clause};