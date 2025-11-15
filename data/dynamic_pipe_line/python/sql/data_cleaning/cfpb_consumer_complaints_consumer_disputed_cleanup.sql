-- Standardizes 'consumer_disputed' by setting empty or NULL values to 'N/A'.
UPDATE consumer_complaints_staging
SET consumer_disputed = 'N/A'
WHERE (consumer_disputed IS NULL OR TRIM(consumer_disputed) = '')
{limit_clause};