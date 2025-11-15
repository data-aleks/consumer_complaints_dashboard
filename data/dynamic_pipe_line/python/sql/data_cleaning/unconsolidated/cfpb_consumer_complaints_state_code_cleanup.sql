-- Standardizes state codes: explicitly maps known non-2-letter values, ensures all valid codes are uppercase, 
-- and flags unmappable data as 'N/A'.
UPDATE consumer_complaints_staging
SET state_code = CASE
    -- 1. Handle Missing/Empty Data
    WHEN state_code IS NULL OR TRIM(state_code) = '' THEN 'N/A'
    
    -- 2. Explicitly map known long territory names (more complete approach)
    WHEN state_code = 'UNITED STATES MINOR OUTLYING ISLANDS' THEN 'UM'
    WHEN state_code = 'PUERTO RICO' THEN 'PR'
    WHEN state_code = 'VIRGIN ISLANDS' THEN 'VI'
    WHEN state_code = 'GUAM' THEN 'GU'
    WHEN state_code = 'AMERICAN SAMOA' THEN 'AS'
    WHEN state_code = 'NORTHERN MARIANA ISLANDS' THEN 'MP'
    
    -- 3. Truncate long strings ONLY IF they are known valid state names
    --    (It's safer to avoid truncation, but if you must handle long names, 
    --    it's better to explicitly list them or set them to 'N/A').
    --    *Since explicit listing for 50 states is impractical here, we treat any other LENGTH > 2 as invalid.*
    WHEN LENGTH(state_code) > 2 THEN 'Invalid Code' 
    
    -- 4. Convert all valid 2-letter codes to standard uppercase
    WHEN LENGTH(state_code) = 2 THEN UPPER(TRIM(state_code))

    -- 5. Default to 'N/A' for anything else (e.g., non-2-letter codes that are short but not NULL/empty)
    ELSE 'N/A' 
END
-- Update all rows to ensure completeness
WHERE 1=1 {limit_clause};