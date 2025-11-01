-- CFPB Consumer Complaints Schema Cleanup: consumer_consent Column
-- Standardizes empty consumer_consent to 'None'

-- Identify available consumer_consent values
SELECT DISTINCT consumer_consent FROM consumer_complaints;

-- Fill in missing consumer_consent with 'None'
UPDATE consumer_complaints
SET consumer_consent = 'None'
WHERE consumer_consent IS NULL OR consumer_consent = 'N/A';

-- Standartize other values
UPDATE consumer_complaints
SET consumer_consent = 'Not provided'
WHERE consumer_consent = 'Consent not provided';

UPDATE consumer_complaints
SET consumer_consent = 'Provided'
WHERE consumer_consent = 'Consent provided';

UPDATE consumer_complaints
SET consumer_consent = 'Withdrawn'
WHERE consumer_consent = 'Consent withdrawn';