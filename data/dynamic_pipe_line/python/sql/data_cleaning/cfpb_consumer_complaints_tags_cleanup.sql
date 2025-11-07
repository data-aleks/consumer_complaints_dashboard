-- CFPB Consumer Complaints Cleanup: tags
-- Standardizes missing or blank values in the cleaned table

-- Optional: Review current values
-- SELECT DISTINCT tags FROM consumer_complaints_cleaned;

-- Replace NULL or blank values with 'None'
UPDATE consumer_complaints_cleaned
SET tags = 'None'
WHERE tags IS NULL OR TRIM(tags) = '';