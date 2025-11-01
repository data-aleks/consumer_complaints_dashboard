-- Create dimension table
CREATE TABLE dim_tag (
  tag_id INT PRIMARY KEY AUTO_INCREMENT,
  tag VARCHAR(50) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_tag (tag)
SELECT DISTINCT tags
FROM consumer_complaints
WHERE tags IS NOT NULL;