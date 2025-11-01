-- This script creates sub_issue dimension table from distinct values in sub_issue column
-- Create dim_sub_issue table
CREATE TABLE dim_sub_issue (
  sub_issue_id INT PRIMARY KEY AUTO_INCREMENT,
  sub_issue_description VARCHAR(150) NOT NULL
);

-- Populate dim_issue with distinct values from sub_issue column in consumer_complaints
INSERT INTO dim_sub_issue (sub_issue_description)
SELECT DISTINCT sub_issue
FROM consumer_complaints
WHERE sub_issue IS NOT NULL;