-- Create dim_sub_product table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_sub_product (
  sub_product_id INT PRIMARY KEY AUTO_INCREMENT,
  sub_product_name VARCHAR(100) NOT NULL
);

-- Insert only new sub_product values from cleaned table
INSERT INTO dim_sub_product (sub_product_name)
SELECT DISTINCT c.sub_product
FROM consumer_complaints_cleaned c
WHERE c.sub_product IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_sub_product d
    WHERE d.sub_product_name = c.sub_product
  );