-- This script creates issue dimension table from distinct values in issue column
-- Create dim_issue table
CREATE TABLE dim_issue (
  issue_id INT PRIMARY KEY AUTO_INCREMENT,
  issue_description VARCHAR(100) NOT NULL
);

-- Populate dim_issue with distinct values from issue column in consumer_complaints
INSERT INTO dim_issue (issue_description)
SELECT DISTINCT issue
FROM consumer_complaints
WHERE issue IS NOT NULL;