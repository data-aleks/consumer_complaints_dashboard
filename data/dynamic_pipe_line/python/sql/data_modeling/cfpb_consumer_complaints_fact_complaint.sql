-- Populates the main fact table by joining the cleaned data with dimension tables.
INSERT IGNORE INTO fact_complaints (
    complaint_id, date_received_key, date_sent_key, product_key, sub_product_key,
    issue_key, sub_issue_key, company_key, state_key, zip_code_key, origin_key,
    company_response_key, public_response_key, consent_key, tag_key, disputed_key, timely_response,
    consumer_complaint_narrative
)
SELECT
    c.complaint_id,
    dr.date_key,
    ds.date_key,
    p.product_key,
    sp.sub_product_key,
    i.issue_key,
    si.sub_issue_key,
    co.company_key,
    s.state_key,
    z.zip_code_key,
    o.origin_key,
    cr.response_key,
    pr.response_key,
    cn.consent_key,
    t.tag_key,
    d.disputed_key,
    c.timely_response,
    c.consumer_complaint_narrative -- This should be the last column from the cleaned table
FROM consumer_complaints_cleaned c
LEFT JOIN dim_date dr ON c.date_received = dr.full_date
LEFT JOIN dim_date ds ON c.date_sent_to_company = ds.full_date
LEFT JOIN dim_product p ON c.product = p.product_name
LEFT JOIN dim_sub_product sp ON c.sub_product = sp.sub_product_name
LEFT JOIN dim_issue i ON c.issue = i.issue_name
LEFT JOIN dim_sub_issue si ON c.sub_issue = si.sub_issue_name
LEFT JOIN dim_company co ON c.company = co.company_name
LEFT JOIN dim_state s ON c.state = s.state_code
LEFT JOIN dim_zip_code z ON c.zip_code = z.zip_code
LEFT JOIN dim_origin o ON c.submitted_via = o.origin_method
LEFT JOIN dim_company_response cr ON c.company_response_to_consumer = cr.response_description
LEFT JOIN dim_public_response pr ON c.company_public_response = pr.response_text
LEFT JOIN dim_consent cn ON c.consumer_consent_provided = cn.consent_status
LEFT JOIN dim_tag t ON c.tags = t.tag_name
LEFT JOIN dim_disputed d ON c.consumer_disputed = d.disputed_status
WHERE c.complaint_id NOT IN (SELECT complaint_id FROM fact_complaints WHERE complaint_id IS NOT NULL)
{incremental_where_clause}
{limit_clause};