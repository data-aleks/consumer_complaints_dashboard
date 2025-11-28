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
    day_of_week_number INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INT,
    day_of_year INT,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT
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
    state_code VARCHAR(4) NOT NULL UNIQUE,
    state_name VARCHAR(50) AS (
        CASE state_code
            WHEN 'AL' THEN 'Alabama' WHEN 'AK' THEN 'Alaska' WHEN 'AZ' THEN 'Arizona' WHEN 'AR' THEN 'Arkansas' WHEN 'CA' THEN 'California'
            WHEN 'CO' THEN 'Colorado' WHEN 'CT' THEN 'Connecticut' WHEN 'DE' THEN 'Delaware' WHEN 'FL' THEN 'Florida' WHEN 'GA' THEN 'Georgia'
            WHEN 'HI' THEN 'Hawaii' WHEN 'ID' THEN 'Idaho' WHEN 'IL' THEN 'Illinois' WHEN 'IN' THEN 'Indiana' WHEN 'IA' THEN 'Iowa'
            WHEN 'KS' THEN 'Kansas' WHEN 'KY' THEN 'Kentucky' WHEN 'LA' THEN 'Louisiana' WHEN 'ME' THEN 'Maine' WHEN 'MD' THEN 'Maryland'
            WHEN 'MA' THEN 'Massachusetts' WHEN 'MI' THEN 'Michigan' WHEN 'MN' THEN 'Minnesota' WHEN 'MS' THEN 'Mississippi' WHEN 'MO' THEN 'Missouri'
            WHEN 'MT' THEN 'Montana' WHEN 'NE' THEN 'Nebraska' WHEN 'NV' THEN 'Nevada' WHEN 'NH' THEN 'New Hampshire' WHEN 'NJ' THEN 'New Jersey'
            WHEN 'NM' THEN 'New Mexico' WHEN 'NY' THEN 'New York' WHEN 'NC' THEN 'North Carolina' WHEN 'ND' THEN 'North Dakota' WHEN 'OH' THEN 'Ohio'
            WHEN 'OK' THEN 'Oklahoma' WHEN 'OR' THEN 'Oregon' WHEN 'PA' THEN 'Pennsylvania' WHEN 'RI' THEN 'Rhode Island' WHEN 'SC' THEN 'South Carolina'
            WHEN 'SD' THEN 'South Dakota' WHEN 'TN' THEN 'Tennessee' WHEN 'TX' THEN 'Texas' WHEN 'UT' THEN 'Utah' WHEN 'VT' THEN 'Vermont'
            WHEN 'VA' THEN 'Virginia' WHEN 'WA' THEN 'Washington' WHEN 'WV' THEN 'West Virginia' WHEN 'WI' THEN 'Wisconsin' WHEN 'WY' THEN 'Wyoming'
            WHEN 'AS' THEN 'American Samoa' WHEN 'DC' THEN 'District of Columbia' WHEN 'GU' THEN 'Guam' WHEN 'MP' THEN 'Northern Mariana Islands'
            WHEN 'PR' THEN 'Puerto Rico' WHEN 'VI' THEN 'U.S. Virgin Islands' WHEN 'UM' THEN 'U.S. Minor Outlying Islands'
            WHEN 'AA' THEN 'Armed Forces Americas' WHEN 'AE' THEN 'Armed Forces Europe' WHEN 'AP' THEN 'Armed Forces Pacific'
            ELSE 'N/A'
        END
    ) STORED,
    country_code VARCHAR(3) AS (
        CASE WHEN state_code IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'AS', 'DC', 'GU', 'MP', 'PR', 'VI', 'UM', 'AA', 'AE', 'AP') THEN 'US' ELSE 'N/A' END
    ) STORED,
    country_name VARCHAR(50) AS (
        CASE WHEN state_code IN ('N/A', 'Invalid Code') THEN 'N/A' ELSE 'United States' END
    ) STORED
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