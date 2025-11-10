-- Populates the product dimension table.
INSERT IGNORE INTO dim_product (product_name)
SELECT DISTINCT product
FROM consumer_complaints_cleaned
WHERE product IS NOT NULL {limit_clause};