-- Populates the sub-product dimension table.
INSERT IGNORE INTO dim_sub_product (sub_product_name)
SELECT DISTINCT sub_product
FROM consumer_complaints_cleaned
WHERE sub_product IS NOT NULL {limit_clause};