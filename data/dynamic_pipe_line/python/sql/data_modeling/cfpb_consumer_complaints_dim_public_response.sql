-- Create dim_public_response table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_public_response (
  public_response_id INT PRIMARY KEY AUTO_INCREMENT,
  public_response VARCHAR(200) NOT NULL
);

-- Insert only new public response values from cleaned table
INSERT INTO dim_public_response (public_response)
SELECT DISTINCT c.company_public_response
FROM consumer_complaints_cleaned c
WHERE c.company_public_response IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_public_response d
    WHERE d.public_response = c.company_public_response
  );