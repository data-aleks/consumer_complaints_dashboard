-- Populates the issue dimension table.
INSERT IGNORE INTO dim_issue (issue_name)
SELECT DISTINCT issue AS issue_name
FROM consumer_complaints_cleaned
WHERE issue IS NOT NULL {limit_clause};