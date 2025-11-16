-- Creates all dimension and fact tables for the data model.

-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_company (
    company_key INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_consent (
    consent_key INT AUTO_INCREMENT PRIMARY KEY,
    consent_status VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE UNIQUE,
    `year` INT,
    `month` INT,
    `day` INT,
    `quarter` INT,
    day_of_week_number INT
);

CREATE TABLE IF NOT EXISTS dim_disputed (
    disputed_key INT AUTO_INCREMENT PRIMARY KEY,
    disputed_status VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_issue (
    issue_key INT AUTO_INCREMENT PRIMARY KEY,
    issue_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_origin (
    origin_key INT AUTO_INCREMENT PRIMARY KEY,
    origin_method VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_public_response (
    response_key INT AUTO_INCREMENT PRIMARY KEY,
    response_text VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_state (
    state_key INT AUTO_INCREMENT PRIMARY KEY,
    state_code VARCHAR(50) UNIQUE,
    state_name VARCHAR(255),
    country_code VARCHAR(10),
    country_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_company_response (
    response_key INT AUTO_INCREMENT PRIMARY KEY,
    response_description VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_sub_issue (
    sub_issue_key INT AUTO_INCREMENT PRIMARY KEY,
    sub_issue_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_sub_product (
    sub_product_key INT AUTO_INCREMENT PRIMARY KEY,
    sub_product_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_tag (
    tag_key INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(255) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_zip_code (
    zip_code_key INT AUTO_INCREMENT PRIMARY KEY,
    zip_code VARCHAR(20) UNIQUE
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_complaints (
    complaint_fact_key INT AUTO_INCREMENT PRIMARY KEY,
    complaint_id INT UNIQUE,
    date_received_key INT,
    date_sent_key INT,
    product_key INT,
    sub_product_key INT,
    issue_key INT,
    sub_issue_key INT,
    company_key INT,
    state_key INT,
    zip_code_key INT,
    origin_key INT,    
    company_response_key INT,
    public_response_key INT,
    consent_key INT,
    tag_key INT,
    disputed_key INT,
    timely_response BOOLEAN,
    consumer_complaint_narrative TEXT,

    -- Foreign Key Constraints
    FOREIGN KEY (date_received_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (date_sent_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (sub_product_key) REFERENCES dim_sub_product(sub_product_key),
    FOREIGN KEY (issue_key) REFERENCES dim_issue(issue_key),
    FOREIGN KEY (sub_issue_key) REFERENCES dim_sub_issue(sub_issue_key),
    FOREIGN KEY (company_key) REFERENCES dim_company(company_key),
    FOREIGN KEY (state_key) REFERENCES dim_state(state_key),
    FOREIGN KEY (zip_code_key) REFERENCES dim_zip_code(zip_code_key),
    FOREIGN KEY (origin_key) REFERENCES dim_origin(origin_key),
    FOREIGN KEY (company_response_key) REFERENCES dim_company_response(response_key),
    FOREIGN KEY (public_response_key) REFERENCES dim_public_response(response_key),
    FOREIGN KEY (consent_key) REFERENCES dim_consent(consent_key),
    FOREIGN KEY (tag_key) REFERENCES dim_tag(tag_key),
    FOREIGN KEY (disputed_key) REFERENCES dim_disputed(disputed_key)
);