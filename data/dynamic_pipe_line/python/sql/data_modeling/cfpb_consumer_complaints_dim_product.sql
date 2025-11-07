-- Create dim_product table if it doesn't exist
CREATE TABLE IF NOT EXISTS dim_product (
  product_id INT PRIMARY KEY AUTO_INCREMENT,
  product_name VARCHAR(100) NOT NULL
);

-- Insert only new product values from cleaned table
INSERT INTO dim_product (product_name)
SELECT DISTINCT c.product
FROM consumer_complaints_cleaned c
WHERE c.product IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM dim_product d
    WHERE d.product_name = c.product
  );