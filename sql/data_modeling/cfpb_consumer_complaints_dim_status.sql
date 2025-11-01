-- Create dimension table
CREATE TABLE dim_status (
  status_id INT PRIMARY KEY AUTO_INCREMENT,
  complaint_status VARCHAR(50) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_status (complaint_status)
SELECT DISTINCT company_response
FROM consumer_complaints
WHERE company_response IS NOT NULL;