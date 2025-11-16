-- Populates the product dimension table.
INSERT IGNORE INTO dim_product (product_name)
SELECT DISTINCT product_standardized AS product
FROM consumer_complaints_cleaned
WHERE product_standardized IS NOT NULL AND TRIM(product_standardized) <> '' {limit_clause};