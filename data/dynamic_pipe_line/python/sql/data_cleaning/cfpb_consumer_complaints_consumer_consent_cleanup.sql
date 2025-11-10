-- Standardizes 'Consumer consent provided?' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET consumer_consent_provided = NULL
WHERE consumer_consent_provided = '' {incremental_clause} {limit_clause};