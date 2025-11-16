-- Populates the status (company response) dimension table.
-- Populates the company response dimension table.
INSERT IGNORE INTO dim_company_response (response_description)
SELECT DISTINCT company_response_to_consumer_standardized AS response_description
FROM consumer_complaints_cleaned
WHERE company_response_to_consumer_standardized IS NOT NULL {limit_clause};