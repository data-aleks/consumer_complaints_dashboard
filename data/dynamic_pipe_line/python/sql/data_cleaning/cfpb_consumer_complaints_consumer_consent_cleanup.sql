-- Standardizes 'Consumer consent provided?' into a consistent set of values.
UPDATE consumer_complaints_raw
SET consumer_consent_provided = CASE
    -- Handle null, empty, or N/A values
    WHEN consumer_consent_provided IS NULL OR TRIM(consumer_consent_provided) = '' OR consumer_consent_provided = 'N/A' THEN 'None'
    -- Standardize known values
    WHEN consumer_consent_provided = 'Consent not provided' THEN 'Not provided'
    WHEN consumer_consent_provided = 'Consent provided' THEN 'Provided'
    WHEN consumer_consent_provided = 'Consent withdrawn' THEN 'Withdrawn'
    ELSE consumer_consent_provided -- Keep the original value if it doesn't match any condition
END
WHERE (
    consumer_consent_provided IS NULL
    OR TRIM(consumer_consent_provided) = ''
    OR consumer_consent_provided IN ('N/A', 'Consent not provided', 'Consent provided', 'Consent withdrawn')
) {incremental_clause} {limit_clause};