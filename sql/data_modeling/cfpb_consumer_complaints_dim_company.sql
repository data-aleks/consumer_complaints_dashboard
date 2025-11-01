-- This script creates company dimension table from distinct values in company_name column
-- Create dim_company table
CREATE TABLE dim_company (
  company_id INT PRIMARY KEY AUTO_INCREMENT,
  company VARCHAR(200) NOT NULL
);

-- Populate dim_company with distinct values from company_name column in consumer_complaints
INSERT INTO dim_company (company)
SELECT DISTINCT company_name
FROM consumer_complaints
WHERE company_name IS NOT NULL;