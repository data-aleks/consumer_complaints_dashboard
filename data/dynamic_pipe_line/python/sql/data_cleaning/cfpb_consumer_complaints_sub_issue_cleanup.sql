-- CFPB Consumer Complaints Cleanup: sub_issue
-- Standardizes missing or blank values in the cleaned table

-- Optional: Review current values
-- SELECT DISTINCT sub_issue FROM consumer_complaints_cleaned;

-- Replace NULL or blank values with 'None'
UPDATE consumer_complaints_cleaned
SET sub_issue = 'None'
WHERE sub_issue IS NULL OR TRIM(sub_issue) = '';