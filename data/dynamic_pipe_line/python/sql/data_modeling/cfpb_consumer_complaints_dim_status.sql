-- Create dim_status table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_status (
  status_id INT PRIMARY KEY AUTO_INCREMENT,
  complaint_status VARCHAR(50) NOT NULL
);

-- Insert only new complaint status values from cleaned table
INSERT INTO dim_status (complaint_status)
SELECT DISTINCT c.company_response
FROM consumer_complaints_cleaned c
WHERE c.company_response IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_status d
    WHERE d.complaint_status = c.company_response
  );