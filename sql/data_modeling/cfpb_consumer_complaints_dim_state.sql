-- Create dimension table
CREATE TABLE dim_state (
  state_id INT PRIMARY KEY AUTO_INCREMENT,
  state_code VARCHAR(2) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_state (state_code)
SELECT DISTINCT state_code
FROM consumer_complaints
WHERE state_code IS NOT NULL;