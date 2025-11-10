-- Populates the tag dimension table.
INSERT IGNORE INTO dim_tag (tag_name)
SELECT DISTINCT tags AS tag_name
FROM consumer_complaints_cleaned
WHERE tags IS NOT NULL {limit_clause};