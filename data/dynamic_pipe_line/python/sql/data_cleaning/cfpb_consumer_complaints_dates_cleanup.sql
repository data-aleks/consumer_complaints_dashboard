-- Converts date columns from text to proper DATE format.
UPDATE consumer_complaints_raw
SET date_received = STR_TO_DATE(date_received, '%%Y-%%m-%%d'),
    date_sent_to_company = STR_TO_DATE(date_sent_to_company, '%%Y-%%m-%%d')
WHERE
    -- Apply to rows where conversion is needed
    date_received IS NOT NULL AND date_sent_to_company IS NOT NULL {incremental_clause} {limit_clause};