-- Create dim_company table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_company (
  company_id INT PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(200) NOT NULL
);

-- Insert only new company names from cleaned table
INSERT INTO dim_company (company)
SELECT DISTINCT c.company_name
FROM consumer_complaints_cleaned c
WHERE c.company_name IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_company d
    WHERE d.company = c.company_name
  );