-- CFPB Consumer Complaints Schema Cleanup: Tags Column
-- Standardizes empty tags to 'None'

-- Identify available tags
SELECT DISTINCT tags FROM consumer_complaints;

-- Fill in missing tags with 'None'
UPDATE consumer_complaints
SET tags = 'None'
WHERE tags IS NULL OR tags = '';