-- This script creates continous date dimension table
-- Create a list of all dates.
SELECT DISTINCT date_received AS date_value
FROM consumer_complaints
WHERE date_received IS NOT NULL
UNION
SELECT DISTINCT date_sent_to_company AS date_value
FROM consumer_complaints
WHERE date_sent_to_company IS NOT NULL
ORDER BY date_value;

-- Create a date dimension table
CREATE TABLE dim_date (
	date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday_name VARCHAR(10),
    is_weekend BOOLEAN
);


-- Populate date attributes into dim_date using inline CTE
SET @@cte_max_recursion_depth = 10000;
INSERT INTO dim_date (full_date, year, quarter, month, day, weekday_name, is_weekend)
WITH RECURSIVE date_range AS (
  SELECT MIN(date_received) AS date_value
  FROM consumer_complaints
  UNION ALL
  SELECT DATE_ADD(date_value, INTERVAL 1 DAY)
  FROM date_range
  WHERE date_value < (
    SELECT MAX(date_sent_to_company)
    FROM consumer_complaints
  )
)
SELECT 
  date_value,
  YEAR(date_value),
  QUARTER(date_value),
  MONTH(date_value),
  DAY(date_value),
  DAYNAME(date_value),
  CASE WHEN DAYOFWEEK(date_value) IN (1,7) THEN TRUE ELSE FALSE END
FROM date_range;
