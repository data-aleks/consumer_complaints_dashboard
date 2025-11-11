-- Standardizes 'Consumer complaint narrative' by setting empty or NULL values to 'None'.
UPDATE consumer_complaints_raw
SET consumer_complaint_narrative = 'None'
WHERE (consumer_complaint_narrative IS NULL OR TRIM(consumer_complaint_narrative) = '') {incremental_clause} {limit_clause};