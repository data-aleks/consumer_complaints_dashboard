-- CFPB Consumer Complaints Cleanup: Date Fields
-- Cleans and converts date_received and date_sent_to_company in the cleaned table

-- Step 1: Nullify junk values in date_received
UPDATE consumer_complaints_cleaned
SET date_received = NULL
WHERE date_received IS NOT NULL
  AND TRIM(date_received) != ''
  AND date_received NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Step 2: Nullify junk values in date_sent_to_company
UPDATE consumer_complaints_cleaned
SET date_sent_to_company = NULL
WHERE date_sent_to_company IS NOT NULL
  AND TRIM(date_sent_to_company) != ''
  AND date_sent_to_company NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Step 3: Convert columns to DATE type (only if safe and needed)
-- Optional: Uncomment if you're ready to enforce DATE type
ALTER TABLE consumer_complaints_cleaned
MODIFY COLUMN date_received DATE;

ALTER TABLE consumer_complaints_cleaned
MODIFY COLUMN date_sent_to_company DATE;