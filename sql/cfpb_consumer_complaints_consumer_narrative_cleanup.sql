-- CFPB Consumer Complaints Schema Cleanup: consumer_narrative column
-- Standardizes consumer_narrative values

-- Select values
SELECT DISTINCT consumer_narrative FROM consumer_complaints;

-- Fill in missing sub_issue values with 'None'
UPDATE consumer_complaints
SET consumer_narrative = 'None'
WHERE consumer_narrative IS NULL OR consumer_narrative = '';