-- Add a timestamp to track when a record is moved to the staging table.
-- This prevents records from being staged multiple times.
ALTER TABLE consumer_complaints_raw
ADD COLUMN staged_timestamp DATETIME NULL,
ADD INDEX idx_raw_staged_timestamp (staged_timestamp);