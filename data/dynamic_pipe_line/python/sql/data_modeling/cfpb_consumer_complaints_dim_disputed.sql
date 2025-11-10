-- Populates the disputed dimension table.
INSERT IGNORE INTO dim_disputed (disputed_status)
SELECT DISTINCT consumer_disputed AS disputed_status
FROM consumer_complaints_cleaned
WHERE consumer_disputed IS NOT NULL {limit_clause};