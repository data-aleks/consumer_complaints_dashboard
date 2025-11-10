-- Populates the status (company response) dimension table.
INSERT IGNORE INTO dim_status (status_description)
SELECT DISTINCT company_response_to_consumer AS status_description
FROM consumer_complaints_cleaned
WHERE company_response_to_consumer IS NOT NULL {limit_clause};