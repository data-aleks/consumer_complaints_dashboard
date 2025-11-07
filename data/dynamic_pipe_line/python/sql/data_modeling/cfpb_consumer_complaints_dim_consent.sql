-- Create dim_consent table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_consent (
  consent_id INT PRIMARY KEY AUTO_INCREMENT,
  consent VARCHAR(50) NOT NULL
);

-- Insert only new consent values from cleaned table
INSERT INTO dim_consent (consent)
SELECT DISTINCT c.consumer_consent
FROM consumer_complaints_cleaned c
WHERE c.consumer_consent IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_consent d
    WHERE d.consent = c.consumer_consent
  );