-- Create fact_complaints table if it doesn't exist
CREATE TABLE IF NOT EXISTS fact_complaints (
  complaint_id INT PRIMARY KEY,

  -- Foreign keys to dimension tables
  date_received_id INT,
  date_sent_id INT,
  product_id INT,
  sub_product_id INT,
  issue_id INT,
  sub_issue_id INT,
  company_id INT,
  state_id INT,
  zip_code_id INT,
  origin_id INT,
  public_response_id INT,
  status_id INT,
  consent_id INT,
  tag_id INT,
  disputed_id INT,

  -- Descriptive attributes
  consumer_narrative TEXT,

  -- Flags
  timely_response_flag BOOLEAN,

  -- Foreign key constraints
  FOREIGN KEY (date_received_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (date_sent_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (sub_product_id) REFERENCES dim_sub_product(sub_product_id),
  FOREIGN KEY (issue_id) REFERENCES dim_issue(issue_id),
  FOREIGN KEY (sub_issue_id) REFERENCES dim_sub_issue(sub_issue_id),
  FOREIGN KEY (company_id) REFERENCES dim_company(company_id),
  FOREIGN KEY (state_id) REFERENCES dim_state(state_id),
  FOREIGN KEY (zip_code_id) REFERENCES dim_zip_code(zip_code_id),
  FOREIGN KEY (origin_id) REFERENCES dim_origin(origin_id),
  FOREIGN KEY (public_response_id) REFERENCES dim_public_response(public_response_id),
  FOREIGN KEY (status_id) REFERENCES dim_status(status_id),
  FOREIGN KEY (consent_id) REFERENCES dim_consent(consent_id),
  FOREIGN KEY (tag_id) REFERENCES dim_tag(tag_id),
  FOREIGN KEY (disputed_id) REFERENCES dim_disputed(disputed_id)
);

-- Insert only new complaints from cleaned table
INSERT INTO fact_complaints (
  complaint_id,
  date_received_id,
  date_sent_id,
  product_id,
  sub_product_id,
  issue_id,
  sub_issue_id,
  company_id,
  state_id,
  zip_code_id,
  origin_id,
  public_response_id,
  status_id,
  consent_id,
  tag_id,
  disputed_id,
  consumer_narrative,
  timely_response_flag
)
SELECT
  cc.complaint_id,
  dr.date_id,
  ds.date_id,
  dp.product_id,
  dsp.sub_product_id,
  di.issue_id,
  dsi.sub_issue_id,
  dc.company_id,
  dst.state_id,
  dz.zip_code_id,
  dor.origin_id,
  dpr.public_response_id,
  dstat.status_id,
  dcon.consent_id,
  dt.tag_id,
  dd.disputed_id,
  cc.consumer_narrative,
  CASE
    WHEN LOWER(TRIM(cc.timely_response)) = 'yes' THEN TRUE
    WHEN LOWER(TRIM(cc.timely_response)) = 'no' THEN FALSE
    ELSE NULL
  END
FROM consumer_complaints_cleaned cc
JOIN dim_date dr ON cc.date_received = dr.full_date
JOIN dim_date ds ON cc.date_sent_to_company = ds.full_date
JOIN dim_product dp ON cc.product = dp.product_name
JOIN dim_sub_product dsp ON cc.sub_product = dsp.sub_product_name
JOIN dim_issue di ON cc.issue = di.issue_description
JOIN dim_sub_issue dsi ON cc.sub_issue = dsi.sub_issue_description
JOIN dim_company dc ON cc.company_name = dc.company
JOIN dim_state dst ON cc.state_code = dst.state_code
JOIN dim_zip_code dz ON cc.zip_code = dz.zip_code
JOIN dim_origin dor ON cc.submitted_via = dor.origin
JOIN dim_public_response dpr ON cc.company_public_response = dpr.public_response
JOIN dim_status dstat ON cc.company_response = dstat.complaint_status
JOIN dim_consent dcon ON cc.consumer_consent = dcon.consent
JOIN dim_tag dt ON cc.tags = dt.tag
JOIN dim_disputed dd ON cc.consumer_disputed = dd.disputed
WHERE NOT EXISTS (
  SELECT 1
  FROM fact_complaints fc
  WHERE fc.complaint_id = cc.complaint_id
);