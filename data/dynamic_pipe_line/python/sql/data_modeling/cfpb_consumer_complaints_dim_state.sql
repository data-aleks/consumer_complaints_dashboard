-- Create dim_state table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_state (
  state_id INT PRIMARY KEY AUTO_INCREMENT,
  state_code VARCHAR(2) NOT NULL
);

-- Insert only new state codes from cleaned table
INSERT INTO dim_state (state_code)
SELECT DISTINCT c.state_code
FROM consumer_complaints_cleaned c
WHERE c.state_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_state d
    WHERE d.state_code = c.state_code
  );