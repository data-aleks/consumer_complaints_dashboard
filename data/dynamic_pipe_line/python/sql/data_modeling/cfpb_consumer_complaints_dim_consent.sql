-- Populates the consent dimension table.
INSERT IGNORE INTO dim_consent (consent_status)
SELECT DISTINCT consumer_consent_provided_standardized
FROM consumer_complaints_cleaned
WHERE consumer_consent_provided_standardized IS NOT NULL {limit_clause};
