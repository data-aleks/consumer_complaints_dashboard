-- Standardizes 'Company response to consumer' using 'N/A' for blanks and consolidating 'Closed with relief' categories.
UPDATE consumer_complaints_staging
SET company_response_to_consumer = CASE
    -- 1. Handle Null/Empty values first, mapping to 'N/A'
    WHEN company_response_to_consumer IS NULL 
    OR TRIM(company_response_to_consumer) = '' 
    THEN 'N/A'

    -- 2. Map specific relief types
    WHEN company_response_to_consumer = 'Closed with monetary relief' 
    THEN 'Monetary Relief'
    
    WHEN company_response_to_consumer = 'Closed with non-monetary relief' 
    THEN 'Non-monetary Relief'

    -- 3. Map general/unspecified relief (this often overlaps with the above, but should be mapped clearly)
    WHEN company_response_to_consumer = 'Closed with relief' 
    THEN 'Unspecified Relief'
    
    -- 4. Map closure outcomes without relief
    WHEN company_response_to_consumer = 'Closed without relief' 
    THEN 'No Relief'

    -- 5. Map informational closure
    WHEN company_response_to_consumer = 'Closed with explanation' 
    THEN 'Explanation Provided'
    
    -- 6. Preserve other values (e.g., 'In progress', 'Closed')
    ELSE company_response_to_consumer 
END
-- Ensure the script runs on all rows to catch all blank/NULL values.
WHERE 1=1
{limit_clause};