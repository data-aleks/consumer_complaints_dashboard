CREATE TABLE IF NOT EXISTS pipeline_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    pipeline_step VARCHAR(255),
    status VARCHAR(50),
    message TEXT,
    duration_seconds DECIMAL(10, 2),
    details JSON
);