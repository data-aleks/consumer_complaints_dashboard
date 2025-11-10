-- Standardizes 'Sub-issue' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET sub_issue = NULL
WHERE sub_issue = '' {incremental_clause} {limit_clause};