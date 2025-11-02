-- Create dimension table
CREATE TABLE dim_disputed (
  disputed_id INT PRIMARY KEY AUTO_INCREMENT,
  disputed VARCHAR(50) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_disputed (disputed)
SELECT DISTINCT consumer_disputed
FROM consumer_complaints
WHERE consumer_disputed IS NOT NULL;