-- CFPB Consumer Complaints Cleanup: company_public_response
-- Standardizes missing values in the cleaned table

-- Optional: Review current values
-- SELECT DISTINCT company_public_response FROM consumer_complaints_cleaned;

-- Update missing or blank values to 'None'
UPDATE consumer_complaints_cleaned
SET company_public_response = 'None'
WHERE company_public_response IS NULL OR TRIM(company_public_response) = '';