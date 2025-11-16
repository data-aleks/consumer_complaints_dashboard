-- Populates the sub-product dimension table.
INSERT IGNORE INTO dim_sub_product (sub_product_name)
SELECT DISTINCT sub_product_standardized AS sub_product_name
FROM consumer_complaints_cleaned
WHERE sub_product_standardized IS NOT NULL AND TRIM(sub_product_standardized) <> '' {limit_clause};