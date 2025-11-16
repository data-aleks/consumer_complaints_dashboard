-- Populates the state dimension table with unique state codes from the cleaned data.
-- INSERT IGNORE prevents errors if a state code already exists.
INSERT IGNORE INTO dim_state (state_code, state_name, country_code, country_name)
SELECT DISTINCT
    c.state_code,
    -- Map state codes to full state names
    CASE c.state_code
        WHEN 'AL' THEN 'Alabama'
        WHEN 'AK' THEN 'Alaska'
        WHEN 'AZ' THEN 'Arizona'
        WHEN 'AR' THEN 'Arkansas'
        WHEN 'CA' THEN 'California'
        WHEN 'CO' THEN 'Colorado'
        WHEN 'CT' THEN 'Connecticut'
        WHEN 'DE' THEN 'Delaware'
        WHEN 'FL' THEN 'Florida'
        WHEN 'GA' THEN 'Georgia'
        WHEN 'HI' THEN 'Hawaii'
        WHEN 'ID' THEN 'Idaho'
        WHEN 'IL' THEN 'Illinois'
        WHEN 'IN' THEN 'Indiana'
        WHEN 'IA' THEN 'Iowa'
        WHEN 'KS' THEN 'Kansas'
        WHEN 'KY' THEN 'Kentucky'
        WHEN 'LA' THEN 'Louisiana'
        WHEN 'ME' THEN 'Maine'
        WHEN 'MD' THEN 'Maryland'
        WHEN 'MA' THEN 'Massachusetts'
        WHEN 'MI' THEN 'Michigan'
        WHEN 'MN' THEN 'Minnesota'
        WHEN 'MS' THEN 'Mississippi'
        WHEN 'MO' THEN 'Missouri'
        WHEN 'MT' THEN 'Montana'
        WHEN 'NE' THEN 'Nebraska'
        WHEN 'NV' THEN 'Nevada'
        WHEN 'NH' THEN 'New Hampshire'
        WHEN 'NJ' THEN 'New Jersey'
        WHEN 'NM' THEN 'New Mexico'
        WHEN 'NY' THEN 'New York'
        WHEN 'NC' THEN 'North Carolina'
        WHEN 'ND' THEN 'North Dakota'
        WHEN 'OH' THEN 'Ohio'
        WHEN 'OK' THEN 'Oklahoma'
  
      WHEN 'OR' THEN 'Oregon'
        WHEN 'PA' THEN 'Pennsylvania'
        WHEN 'RI' THEN 'Rhode Island'
        WHEN 'SC' THEN 'South Carolina'
        WHEN 'SD' THEN 'South Dakota'
        WHEN 'TN' THEN 'Tennessee'
        WHEN 'TX' THEN 'Texas'
        WHEN 'UT' THEN 'Utah'
        WHEN 'VT' THEN 'Vermont'
        WHEN 'VA' THEN 'Virginia'
        WHEN 'WA' THEN 'Washington'
        WHEN 'WV' THEN 'West Virginia'
        WHEN 'WI' THEN 'Wisconsin'
        WHEN 'WY' THEN 'Wyoming'
        -- Territories
        WHEN 'AS' THEN 'American Samoa'
        WHEN 'DC' THEN 'District of Columbia'
        WHEN 'GU' THEN 'Guam'
        WHEN 'MP' THEN 'Northern Mariana Islands'
        WHEN 'PR' THEN 'Puerto Rico'
        WHEN 'VI' THEN 'U.S. Virgin Islands'
        WHEN 'UM' THEN 'U.S. Minor Outlying Islands'
        -- Armed Forces
        WHEN 'AA' THEN 'Armed Forces Americas'
        WHEN 'AE' THEN 'Armed Forces Europe'
        WHEN 'AP' THEN 'Armed Forces Pacific'
        ELSE 'N/A' -- Use N/A for codes that are not standard US states/territories
    END AS state_name,
    -- Add country code and name based on the state code
    CASE
        WHEN c.state_code IN (
            -- States
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 
            -- Territories & Armed Forces
            'AS', 'DC', 'GU', 'MP', 'PR', 'VI', 'UM', 'AA', 'AE', 'AP'
        ) THEN 'US'
        ELSE 'N/A'
    END AS country_code,
    CASE WHEN c.state_code IN ('N/A', 'Invalid Code') THEN 'N/A' ELSE 'United States' END AS country_name
FROM consumer_complaints_cleaned c
WHERE c.state_code IS NOT NULL AND TRIM(c.state_code) <> '' {incremental_clause};