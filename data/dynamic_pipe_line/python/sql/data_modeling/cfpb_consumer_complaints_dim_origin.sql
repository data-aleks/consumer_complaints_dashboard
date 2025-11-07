-- Create dim_origin table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_origin (
  origin_id INT PRIMARY KEY AUTO_INCREMENT,
  origin VARCHAR(50) NOT NULL
);

-- Insert only new origin values from cleaned table
INSERT INTO dim_origin (origin)
SELECT DISTINCT c.submitted_via
FROM consumer_complaints_cleaned c
WHERE c.submitted_via IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_origin d
    WHERE d.origin = c.submitted_via
  );