-- Create dimension table
CREATE TABLE dim_zip_code (
  zip_code_id INT PRIMARY KEY AUTO_INCREMENT,
  zip_code VARCHAR(11) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_zip_code (zip_code)
SELECT DISTINCT zip_code
FROM consumer_complaints
WHERE zip_code IS NOT NULL;