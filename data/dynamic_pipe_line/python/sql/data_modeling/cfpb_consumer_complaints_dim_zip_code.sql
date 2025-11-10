-- Populates the zip code dimension table.
INSERT IGNORE INTO dim_zip_code (zip_code)
SELECT DISTINCT zip_code
FROM consumer_complaints_cleaned
WHERE zip_code IS NOT NULL {limit_clause};