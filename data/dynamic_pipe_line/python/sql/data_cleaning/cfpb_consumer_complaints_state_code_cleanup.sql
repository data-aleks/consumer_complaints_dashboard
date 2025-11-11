-- Nullifies invalid state codes.
-- Standardizes state codes by replacing long names and truncating invalid lengths.
UPDATE consumer_complaints_raw
SET state = CASE
    -- Replace full name with valid 2-letter code
    WHEN state = 'UNITED STATES MINOR OUTLYING ISLANDS' THEN 'UM'
    -- Truncate any values longer than 2 characters as a fallback
    WHEN LENGTH(state) > 2 THEN LEFT(state, 2)
    ELSE state -- Keep the original value if it's already valid
END
WHERE
    (state = 'UNITED STATES MINOR OUTLYING ISLANDS' OR LENGTH(state) > 2) {incremental_clause} {limit_clause};