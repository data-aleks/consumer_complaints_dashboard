-- Populates the state dimension table.
INSERT IGNORE INTO dim_state (state_code)
SELECT DISTINCT c.state AS state_code
FROM consumer_complaints_cleaned c
WHERE c.state IS NOT NULL
{limit_clause};