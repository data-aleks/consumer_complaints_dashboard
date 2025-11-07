-- Create dim_issue table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_issue (
  issue_id INT PRIMARY KEY AUTO_INCREMENT,
  issue_description VARCHAR(100) NOT NULL
);

-- Insert only new issue values from cleaned table
INSERT INTO dim_issue (issue_description)
SELECT DISTINCT c.issue
FROM consumer_complaints_cleaned c
WHERE c.issue IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_issue d
    WHERE d.issue_description = c.issue
  );