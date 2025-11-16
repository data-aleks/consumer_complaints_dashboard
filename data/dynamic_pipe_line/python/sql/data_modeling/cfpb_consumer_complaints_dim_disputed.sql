-- Populates the disputed dimension table.
INSERT IGNORE INTO dim_disputed (disputed_status)
SELECT DISTINCT consumer_disputed_standardized AS disputed_status
FROM consumer_complaints_cleaned
WHERE consumer_disputed_standardized IS NOT NULL {limit_clause};