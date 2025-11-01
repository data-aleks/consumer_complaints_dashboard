-- CFPB Consumer Complaints Schema Cleanup: Company Response Column
-- Standardizes company_response values

-- Explanation
UPDATE consumer_complaints
SET company_response = 'Explanation'
WHERE company_response = 'Closed with explanation';

-- Non-monetary relief
UPDATE consumer_complaints
SET company_response = 'Non-monetary relief'
WHERE company_response = 'Closed with non-monetary relief';

-- Monetary relief
UPDATE consumer_complaints
SET company_response = 'Monetary relief'
WHERE company_response = 'Closed with monetary relief';

-- Generic relief
UPDATE consumer_complaints
SET company_response = 'Relief'
WHERE company_response = 'Closed with relief';

-- No relief
UPDATE consumer_complaints
SET company_response = 'No relief'
WHERE company_response = 'Closed without relief';

-- Closed
UPDATE consumer_complaints
SET company_response = 'Closed'
WHERE company_response = 'Closed';

-- In progress
UPDATE consumer_complaints
SET company_response = 'In progress'
WHERE company_response = 'In progress';

-- Untimely response
UPDATE consumer_complaints
SET company_response = 'Untimely response'
WHERE company_response = 'Untimely response';