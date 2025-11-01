-- Create dimension table
CREATE TABLE dim_origin (
  origin_id INT PRIMARY KEY AUTO_INCREMENT,
  origin VARCHAR(50) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_origin (origin)
SELECT DISTINCT submitted_via
FROM consumer_complaints
WHERE submitted_via IS NOT NULL;