-- CFPB Consumer Complaints Schema Cleanup: State Code
-- Cleans and standardizes values in the state_code column
-- Intended for use with MySQL 8.0+

-- Identify state_code values longer than 2 characters
SELECT complaint_id, state_code
FROM consumer_complaints
WHERE LENGTH(state_code) > 2;

-- Replace full name with valid 2-letter code
-- UNITED STATES MINOR OUTLYING ISLANDS → UM
UPDATE consumer_complaints
SET state_code = 'UM'
WHERE state_code = 'UNITED STATES MINOR OUTLYING ISLANDS';

