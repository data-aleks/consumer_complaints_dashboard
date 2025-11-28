CREATE TABLE IF NOT EXISTS ingestion_metadata (
    source_file_name VARCHAR(255) NOT NULL,
    file_hash CHAR(64) NOT NULL PRIMARY KEY,
    row_count INT NOT NULL,
    max_complaint_id BIGINT NOT NULL,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_modified_date DATETIME NULL
);