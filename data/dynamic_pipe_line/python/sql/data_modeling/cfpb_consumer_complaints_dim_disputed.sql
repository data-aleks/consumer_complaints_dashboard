-- Create dim_disputed table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_disputed (
  disputed_id INT PRIMARY KEY AUTO_INCREMENT,
  disputed VARCHAR(50) NOT NULL
);

-- Insert only new disputed values from cleaned table
INSERT INTO dim_disputed (disputed)
SELECT DISTINCT c.consumer_disputed
FROM consumer_complaints_cleaned c
WHERE c.consumer_disputed IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_disputed d
    WHERE d.disputed = c.consumer_disputed
  );