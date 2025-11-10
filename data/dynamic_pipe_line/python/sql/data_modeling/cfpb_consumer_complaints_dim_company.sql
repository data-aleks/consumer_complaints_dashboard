-- Populates the company dimension table.
INSERT IGNORE INTO dim_company (company_name)
SELECT DISTINCT c.company
FROM consumer_complaints_cleaned c
WHERE c.company IS NOT NULL
{limit_clause};