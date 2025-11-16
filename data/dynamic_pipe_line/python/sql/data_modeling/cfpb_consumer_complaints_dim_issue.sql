-- Populates the issue dimension table.
INSERT IGNORE INTO dim_issue (issue_name)
SELECT DISTINCT issue_standardized AS issue_name
FROM consumer_complaints_cleaned
WHERE issue_standardized IS NOT NULL AND TRIM(issue_standardized) <> '' {limit_clause};