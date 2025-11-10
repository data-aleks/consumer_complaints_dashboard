-- Populates the consent dimension table.
INSERT IGNORE INTO dim_consent (consent_status)
SELECT DISTINCT consumer_consent_provided
FROM consumer_complaints_cleaned
WHERE consumer_consent_provided IS NOT NULL {limit_clause};
