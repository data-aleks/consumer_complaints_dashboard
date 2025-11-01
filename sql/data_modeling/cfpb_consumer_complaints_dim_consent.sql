-- Create dimension table
CREATE TABLE dim_consent (
  consent_id INT PRIMARY KEY AUTO_INCREMENT,
  consent VARCHAR(50) NOT NULL
);

-- Populate dimension table
INSERT INTO dim_consent (consent)
SELECT DISTINCT consumer_consent
FROM consumer_complaints
WHERE consumer_consent IS NOT NULL;