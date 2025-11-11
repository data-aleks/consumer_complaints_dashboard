-- Populates the status (company response) dimension table.
-- Populates the company response dimension table.
INSERT IGNORE INTO dim_company_response (response_description)
SELECT DISTINCT company_response_to_consumer AS response_description
FROM consumer_complaints_cleaned
WHERE company_response_to_consumer IS NOT NULL {limit_clause};