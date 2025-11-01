-- This script creates sub product dimension table from distinct values in sub_product column
-- Create dim_sub_product table
CREATE TABLE dim_sub_product (
  sub_product_id INT PRIMARY KEY AUTO_INCREMENT,
  sub_product_name VARCHAR(100) NOT NULL
);

-- Populate dim_product with distinct values from sub_product column in consumer_complaints
INSERT INTO dim_sub_product (sub_product_name)
SELECT DISTINCT sub_product
FROM consumer_complaints
WHERE sub_product IS NOT NULL;