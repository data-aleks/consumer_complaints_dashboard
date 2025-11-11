-- Standardize 'timely_response' to boolean-like values (1 for Yes, 0 for No)
UPDATE consumer_complaints_raw
SET timely_response = CASE
    WHEN UPPER(timely_response) = 'YES' THEN 1
    WHEN UPPER(timely_response) = 'NO' THEN 0
    ELSE NULL
END
WHERE cleaned_timestamp IS NULL
  {incremental_clause}
  {limit_clause};