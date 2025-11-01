-- This script creates company public response dimension table from distinct values in company_public_response column
-- Create dim_public_response table
CREATE TABLE dim_public_response (
  public_response_id INT PRIMARY KEY AUTO_INCREMENT,
  public_response VARCHAR(200) NOT NULL
);

-- Populate dim_public_response with distinct values from company_public_response column in consumer_complaints
INSERT INTO dim_public_response (public_response)
SELECT DISTINCT company_public_response
FROM consumer_complaints
WHERE company_public_response IS NOT NULL;