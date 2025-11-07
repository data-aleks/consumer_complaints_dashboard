-- CFPB Consumer Complaints Cleanup: consumer_narrative
-- Standardizes missing or blank values in the cleaned table

-- Optional: Review current values
-- SELECT DISTINCT consumer_narrative FROM consumer_complaints_cleaned;

-- Replace NULL or blank values with 'None'
UPDATE consumer_complaints_cleaned
SET consumer_narrative = 'None'
WHERE consumer_narrative IS NULL OR TRIM(consumer_narrative) = '';