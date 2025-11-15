-- Standardizes the 'timely_response' column to a boolean integer (1 for Yes, 0 for No).
UPDATE consumer_complaints_staging
SET timely_response = CASE
    -- Convert 'Yes' to 1
    WHEN UPPER(TRIM(timely_response)) = 'YES' THEN 1
    
    -- Convert 'No' to 0
    WHEN UPPER(TRIM(timely_response)) = 'NO' THEN 0
    
    -- Handle NULL, empty, or other unexpected values by setting them to NULL, as the timeliness is unknown.
    ELSE NULL 
END
-- This WHERE clause is a placeholder for the limit functionality.
WHERE 1=1 {limit_clause};