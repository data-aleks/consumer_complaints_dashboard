-- Populates the date dimension table from both date columns.
INSERT IGNORE INTO dim_date (full_date, `year`, `month`, `day`, `quarter`, day_of_week_number)
SELECT DISTINCT
    dates.full_date,
    YEAR(dates.full_date),
    MONTH(dates.full_date),
    DAY(dates.full_date),
    QUARTER(dates.full_date),
    DAYOFWEEK(dates.full_date)
FROM (
    SELECT date_received AS full_date FROM consumer_complaints_cleaned WHERE date_received IS NOT NULL
    UNION
    SELECT date_sent_to_company AS full_date FROM consumer_complaints_cleaned WHERE date_sent_to_company IS NOT NULL
) AS dates
{limit_clause};