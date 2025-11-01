-- This script creates product dimension table from distinct values in product column
-- Create dim_product table
CREATE TABLE dim_product (
  product_id INT PRIMARY KEY AUTO_INCREMENT,
  product_name VARCHAR(100) NOT NULL
);

-- Populate dim_product with distinct values from product column in consumer_complaints
INSERT INTO dim_product (product_name)
SELECT DISTINCT product
FROM consumer_complaints
WHERE product IS NOT NULL;