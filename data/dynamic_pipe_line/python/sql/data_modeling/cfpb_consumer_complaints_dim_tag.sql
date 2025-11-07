-- Create dim_tag table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_tag (
  tag_id INT PRIMARY KEY AUTO_INCREMENT,
  tag VARCHAR(50) NOT NULL
);

-- Insert only new tag values from cleaned table
INSERT INTO dim_tag (tag)
SELECT DISTINCT c.tags
FROM consumer_complaints_cleaned c
WHERE c.tags IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_tag d
    WHERE d.tag = c.tags
  );