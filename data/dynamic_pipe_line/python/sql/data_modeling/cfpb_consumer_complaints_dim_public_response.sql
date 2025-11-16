-- Populates the public response dimension table.
INSERT IGNORE INTO dim_public_response (response_text)
SELECT DISTINCT company_public_response_standardized AS response_text
FROM consumer_complaints_cleaned
WHERE company_public_response_standardized IS NOT NULL {limit_clause};