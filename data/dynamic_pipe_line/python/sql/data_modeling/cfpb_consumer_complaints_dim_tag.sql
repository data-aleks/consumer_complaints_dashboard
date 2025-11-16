-- Populates the tag dimension table.
INSERT IGNORE INTO dim_tag (tag_name)
SELECT DISTINCT tags_standardized AS tag_name
FROM consumer_complaints_cleaned
WHERE tags_standardized IS NOT NULL {limit_clause};