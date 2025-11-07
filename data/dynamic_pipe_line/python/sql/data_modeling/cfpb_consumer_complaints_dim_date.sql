-- Create dim_date table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT PRIMARY KEY AUTO_INCREMENT,
  full_date DATE NOT NULL,
  year INT,
  quarter INT,
  month INT,
  month_name VARCHAR(15),
  day INT,
  weekday_name VARCHAR(10),
  is_weekend BOOLEAN
);

-- Insert only new dates into dim_date
INSERT INTO dim_date (
  full_date, year, quarter, month, month_name, day, weekday_name, is_weekend
)
SELECT
  date_value,
  YEAR(date_value),
  QUARTER(date_value),
  MONTH(date_value),
  MONTHNAME(date_value),
  DAY(date_value),
  DAYNAME(date_value),
  CASE WHEN DAYOFWEEK(date_value) IN (1,7) THEN TRUE ELSE FALSE END
FROM (
  SELECT DISTINCT DATE(date_received) AS date_value
  FROM consumer_complaints_cleaned
  WHERE date_received IS NOT NULL
  UNION
  SELECT DISTINCT DATE(date_sent_to_company) AS date_value
  FROM consumer_complaints_cleaned
  WHERE date_sent_to_company IS NOT NULL
) AS all_dates
WHERE date_value NOT IN (
  SELECT full_date FROM dim_date
);