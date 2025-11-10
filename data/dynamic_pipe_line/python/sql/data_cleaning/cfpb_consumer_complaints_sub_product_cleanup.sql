-- Standardizes 'Sub-product' by setting empty strings to NULL.
UPDATE consumer_complaints_raw
SET sub_product = NULL
WHERE sub_product = '' {incremental_clause} {limit_clause};