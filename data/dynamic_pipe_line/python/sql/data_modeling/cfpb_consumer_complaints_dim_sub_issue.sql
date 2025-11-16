-- Populates the sub-issue dimension table.
INSERT IGNORE INTO dim_sub_issue (sub_issue_name)
SELECT DISTINCT sub_issue_standardized AS sub_issue_name
FROM consumer_complaints_cleaned
WHERE sub_issue_standardized IS NOT NULL AND TRIM(sub_issue_standardized) <> '' {limit_clause};