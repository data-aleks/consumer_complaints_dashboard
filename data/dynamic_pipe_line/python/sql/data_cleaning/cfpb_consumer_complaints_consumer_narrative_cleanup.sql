-- Standardizes 'Consumer complaint narrative' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET consumer_complaint_narrative = NULL
WHERE consumer_complaint_narrative = '' {incremental_clause} {limit_clause};