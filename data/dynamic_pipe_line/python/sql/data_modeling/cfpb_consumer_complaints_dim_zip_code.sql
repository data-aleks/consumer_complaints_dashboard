-- Create dim_zip_code table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_zip_code (
  zip_code_id INT PRIMARY KEY AUTO_INCREMENT,
  zip_code VARCHAR(11) NOT NULL
);

-- Insert only new zip codes from cleaned table
INSERT INTO dim_zip_code (zip_code)
SELECT DISTINCT c.zip_code
FROM consumer_complaints_cleaned c
WHERE c.zip_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_zip_code d
    WHERE d.zip_code = c.zip_code
  );