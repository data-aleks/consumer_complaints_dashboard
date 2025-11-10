-- Populates the origin (submitted via) dimension table.
INSERT IGNORE INTO dim_origin (origin_method)
SELECT DISTINCT submitted_via AS origin_method
FROM consumer_complaints_cleaned
WHERE submitted_via IS NOT NULL {limit_clause};