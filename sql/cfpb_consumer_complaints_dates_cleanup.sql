-- CFPB Consumer Complaints Schema Cleanup: Date Fields
-- Cleans and converts date_received and date_sent_to_company
-- Intended for use with MySQL 8.0+

-- Identify invalid date_received entries
SELECT complaint_id, date_received
FROM consumer_complaints
WHERE date_received IS NOT NULL
  AND date_received != ''
  AND date_received NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Identify invalid date_sent_to_company entries
SELECT complaint_id, date_sent_to_company
FROM consumer_complaints
WHERE date_sent_to_company IS NOT NULL
  AND date_sent_to_company != ''
  AND date_sent_to_company NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Remove junk values from date_received
DELETE FROM consumer_complaints
WHERE date_received IS NOT NULL
  AND date_received != ''
  AND date_received NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Remove junk values from date_sent_to_company
DELETE FROM consumer_complaints
WHERE date_sent_to_company IS NOT NULL
  AND date_sent_to_company != ''
  AND date_sent_to_company NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- Convert cleaned date_received to DATE type
ALTER TABLE consumer_complaints
MODIFY COLUMN date_received DATE;

-- Convert cleaned date_sent_to_company to DATE type
ALTER TABLE consumer_complaints
MODIFY COLUMN date_sent_to_company DATE;