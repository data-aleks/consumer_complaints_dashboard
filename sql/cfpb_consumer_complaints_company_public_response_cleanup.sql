-- CFPB Consumer Complaints Schema Cleanup: company_public_response column
-- Standardizes company_public_response values

-- Select values
SELECT DISTINCT company_public_response FROM consumer_complaints;

-- Fill in missing sub_issue values with 'None'
UPDATE consumer_complaints
SET company_public_response = 'None'
WHERE company_public_response IS NULL OR company_public_response = '';