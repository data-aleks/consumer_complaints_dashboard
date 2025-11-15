-- Standardizes 'Consumer consent provided?' into four consistent values: Provided, Not provided, Withdrawn, and N/A (for missing data).
UPDATE consumer_complaints_staging
SET consumer_consent_provided = CASE
    -- 1. Handle Null, Empty, or N/A values first, mapping to 'N/A'
    WHEN consumer_consent_provided IS NULL 
    OR TRIM(consumer_consent_provided) = '' 
    OR consumer_consent_provided = 'N/A' 
    THEN 'N/A'
    
    -- 2. Standardize all forms of 'Provided'
    WHEN consumer_consent_provided = 'Consent provided' 
    THEN 'Provided'
    
    -- 3. Standardize 'Not provided'
    WHEN consumer_consent_provided = 'Consent not provided' 
    THEN 'Not provided'
    
    -- 4. Standardize 'Withdrawn'
    WHEN consumer_consent_provided = 'Consent withdrawn' 
    THEN 'Withdrawn'
    
    -- 5. Default to the existing value if it's already one of the clean values or an unexpected value
    ELSE consumer_consent_provided 
END
-- Ensure the script runs on all rows to catch all N/A, NULL, and original values.
WHERE 1=1
{limit_clause};