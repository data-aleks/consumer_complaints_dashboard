-- CFPB Consumer Complaints Cleanup: state_code
-- Standardizes values in the cleaned table

-- Optional: Review non-standard values
-- SELECT complaint_id, state_code
-- FROM consumer_complaints_cleaned
-- WHERE LENGTH(state_code) > 2;

-- Replace full name with valid 2-letter code
UPDATE consumer_complaints_cleaned
SET state_code = 'UM'
WHERE state_code = 'UNITED STATES MINOR OUTLYING ISLANDS';

-- Truncate any remaining values longer than 2 characters (fallback)
UPDATE consumer_complaints_cleaned
SET state_code = LEFT(state_code, 2)
WHERE LENGTH(state_code) > 2;

-- Optional: Nullify clearly invalid entries
UPDATE consumer_complaints_cleaned
SET state_code = NULL
WHERE state_code IN ('N/A', 'Unknown', 'Other');