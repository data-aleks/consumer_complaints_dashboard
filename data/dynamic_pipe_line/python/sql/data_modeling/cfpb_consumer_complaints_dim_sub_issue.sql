-- Populates the sub-issue dimension table.
INSERT IGNORE INTO dim_sub_issue (sub_issue_name)
SELECT DISTINCT sub_issue AS sub_issue_name
FROM consumer_complaints_cleaned
WHERE sub_issue IS NOT NULL {limit_clause};