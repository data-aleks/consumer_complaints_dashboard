-- CFPB Consumer Complaints Cleanup: consumer_consent
-- Standardizes values in the cleaned table

-- Optional: Review current values
-- SELECT DISTINCT consumer_consent FROM consumer_complaints_cleaned;

-- Fill in missing or ambiguous values with 'None'
UPDATE consumer_complaints_cleaned
SET consumer_consent = 'None'
WHERE consumer_consent IS NULL OR TRIM(consumer_consent) = '' OR consumer_consent = 'N/A';

-- Standardize known values
UPDATE consumer_complaints_cleaned
SET consumer_consent = 'Not provided'
WHERE consumer_consent = 'Consent not provided';

UPDATE consumer_complaints_cleaned
SET consumer_consent = 'Provided'
WHERE consumer_consent = 'Consent provided';

UPDATE consumer_complaints_cleaned
SET consumer_consent = 'Withdrawn'
WHERE consumer_consent = 'Consent withdrawn';