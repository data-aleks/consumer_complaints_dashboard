-- Add a column to lock rows during the staging process, preventing race conditions.
ALTER TABLE consumer_complaints_raw
ADD COLUMN staging_run_id VARCHAR(255) NULL,
ADD INDEX idx_raw_staging_run_id (staging_run_id);