-- CFPB Consumer Complaints Schema Cleanup: sub_issue Column
-- Standardizes sub_issue values

-- Select values
SELECT DISTINCT sub_issue FROM consumer_complaints;

-- Fill in missing sub_issue values with 'None'
UPDATE consumer_complaints
SET sub_issue = 'None'
WHERE sub_issue IS NULL OR sub_issue = '';