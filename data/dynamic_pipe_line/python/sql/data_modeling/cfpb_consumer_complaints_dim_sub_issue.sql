-- Create dim_sub_issue table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_sub_issue (
  sub_issue_id INT PRIMARY KEY AUTO_INCREMENT,
  sub_issue_description VARCHAR(150) NOT NULL
);

-- Insert only new sub_issue values from cleaned table
INSERT INTO dim_sub_issue (sub_issue_description)
SELECT DISTINCT c.sub_issue
FROM consumer_complaints_cleaned c
WHERE c.sub_issue IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_sub_issue d
    WHERE d.sub_issue_description = c.sub_issue
  );