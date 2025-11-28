CREATE TABLE IF NOT EXISTS consumer_complaints_cleaned (
    date_received DATE,
    product VARCHAR(255), -- Original value
    product_standardized VARCHAR(255), -- Standardized category
    sub_product VARCHAR(255), -- Original value
    sub_product_standardized VARCHAR(255), -- Standardized category
    issue VARCHAR(255), -- Original value
    issue_standardized VARCHAR(255), -- Standardized category
    sub_issue VARCHAR(255), -- Original value
    sub_issue_standardized VARCHAR(255), -- Standardized category
    consumer_complaint_narrative TEXT,
    company_public_response TEXT,
    company VARCHAR(255),
    state_code VARCHAR(50),
    zip_code VARCHAR(10), 
    tags TEXT, -- Original value
    tags_standardized VARCHAR(255), -- Standardized category
    consumer_consent_provided VARCHAR(100), -- Original value
    consumer_consent_provided_standardized VARCHAR(255), -- Standardized category
    submitted_via VARCHAR(100),
    date_sent_to_company DATE,
    company_response_to_consumer TEXT, -- Original value
    company_response_to_consumer_standardized VARCHAR(255), -- Standardized category
    timely_response TINYINT(1),
    consumer_disputed VARCHAR(100), -- Original value
    consumer_disputed_standardized VARCHAR(255), -- Standardized category
    company_public_response_standardized VARCHAR(255), -- Standardized category
    complaint_id INT PRIMARY KEY,    
    content_hash VARCHAR(64)
);