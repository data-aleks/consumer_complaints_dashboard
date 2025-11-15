-- Standardizes 'Tags' by mapping known values and setting empty/nulls to 'General'
UPDATE consumer_complaints_staging
SET tags = CASE
    WHEN tags IS NULL OR TRIM(tags) = '' THEN 'General'

    -- Standardized tag categories
    WHEN tags = 'Older American' THEN 'Older American'
    WHEN tags = 'Servicemember' THEN 'Servicemember'
    WHEN tags = 'Older American, Servicemember' THEN 'Older American & Servicemember'

    ELSE tags -- Preserve original if no match
END
WHERE 1=1
{limit_clause};