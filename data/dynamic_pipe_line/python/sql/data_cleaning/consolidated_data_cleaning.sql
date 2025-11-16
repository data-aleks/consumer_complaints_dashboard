-- This single, consolidated script performs all cleaning operations in one pass for maximum performance.
UPDATE consumer_complaints_staging
SET
    -- 2. Clean 'state_code'
    state_code = CASE
        WHEN state_code IS NULL OR TRIM(state_code) = '' THEN 'N/A'
        WHEN state_code = 'UNITED STATES MINOR OUTLYING ISLANDS' THEN 'UM'
        WHEN state_code = 'PUERTO RICO' THEN 'PR'
        WHEN state_code = 'VIRGIN ISLANDS' THEN 'VI'
        WHEN state_code = 'GUAM' THEN 'GU'
        WHEN state_code = 'AMERICAN SAMOA' THEN 'AS'
        WHEN state_code = 'NORTHERN MARIANA ISLANDS' THEN 'MP'
        WHEN LENGTH(state_code) > 2 THEN 'Invalid Code'
        WHEN LENGTH(state_code) = 2 THEN UPPER(TRIM(state_code))
        ELSE 'N/A'
    END,

    -- 3. Clean 'issue'
    issue_standardized = CASE -- Standardize 'issue' into broader, more consistent categories.
        WHEN issue IN (
            'Incorrect information on your report', 'Incorrect information on credit report',
            'Problem with a company''s investigation into an existing problem',
            'Problem with a credit reporting company\'s investigation into an existing problem',
            'Credit reporting company\'s investigation', 'Problem with a company\'s investigation into an existing issue',
            'Improper use of your report', 'Improper use of my credit report',
            'Problem with a purchase shown on your statement'
        ) THEN 'Credit Reporting & Investigation'
        WHEN issue = 'Credit Reporting, Fraud, or Identity Theft' THEN 'Fraud & Identity Theft'
        WHEN issue IN (
            'Attempts to collect debt not owed', 'Cont\'d attempts collect debt not owed',
            'False statements or representation', 'Written notification about debt',
            'Disclosure verification of debt'
        ) THEN 'Debt Validity & Disputed Debt'
        WHEN issue IN (
            'Communication tactics', 'Electronic communications',
            'Threatened to contact someone or share information improperly',
            'Taking/threatening an illegal action'
        ) THEN 'Collection Communication & Tactics'
        WHEN issue IN (
            'Struggling to pay mortgage', 'Struggling to repay your loan', 'Can\'t repay my loan',
            'Struggling to pay your loan', 'Struggling to pay your bill'
        ) THEN 'Loan Hardship & Repayment'
        WHEN issue IN (
            'Managing an account', 'Opening an account', 'Closing your account',
            'Closing an account', 'Problem getting a card or closing an account',
            'Problem accessing account', 'Problem with a purchase or transfer'
        ) THEN 'Account/Card Management & Access'
        WHEN issue IN (
            'Getting a loan', 'Applying for a mortgage or refinancing an existing mortgage',
            'Closing on a mortgage', 'Getting a loan or lease', 'Getting a credit card'
        ) THEN 'Loan Origination & Application'
        WHEN issue IN (
            'Fees or interest', 'Problem caused by your funds being low', 'Problem with overdraft'
        ) THEN 'Fees, Interest & Overdraft'
        WHEN issue IN (
            'Advertising and marketing, including promotional offers', 'Advertising & Disclosure', 'Advertising'
        ) THEN 'Disclosure & Marketing'
        WHEN issue IN (
            'Customer Service & Communication', 'Dealing with your lender or servicer',
            'Dealing with my lender or servicer'
        ) THEN 'Customer Service & Servicing Contact'
        WHEN issue IN (
            'Trouble using the card', 'Trouble using your card', 'Transaction & Payment Issues',
            'Trouble during payment process', 'Problem when making payments',
            'Problem with a lender or other company charging your account'
        ) THEN 'Transactional & Card Functionality'
        WHEN issue IN (
            'Repossession', 'Problems at the end of the loan or lease', 'Managing the loan or lease'
        ) THEN 'End-of-Loan Lifecycle'
        WHEN issue IS NULL OR TRIM(issue) = '' THEN 'N/A'
        ELSE 'Other/Miscellaneous'
    END,
        sub_issue_standardized = CASE
        -- 🚨 FIX: Maps empty strings, NULL, and generic 'Unknown' values to 'Not available'
        WHEN TRIM(sub_issue) = '' OR sub_issue IS NULL OR sub_issue IN (
            'I do not know', 'Unknown', 'Other', 'Other service problem', 'Other transaction problem',
            'Other transaction issues', 'Other service issues', 'Other features, terms, or problems'
        ) THEN 'Other'

        -- 4. Fees & Charges
        WHEN sub_issue IN (
            'Unexpected fees', 'Unexpected or other fees', 'Unexpected/Other fees',
            'Overlimit fee', 'Late fee', 'Excessive fees',
            'Charged upfront or unexpected fees', 'Charged fees or interest you didn''t expect',
            'Charged fees or interest I didn''t expect', 'Other fee'
        ) THEN 'Fees & Charges'

        -- Other Products & Services (Loan Disbursement)
        WHEN sub_issue IN (
            'Was approved for a loan, but didn''t receive the money',
            'Was approved for a loan, but didn''t receive money', -- Note: Typo in original data
            'Applied for loan/did not receive money',
            'Money was not available when promised'
        ) THEN 'Other Products & Services (Disbursement)'

        -- 11. Loan Servicing & Modification (Focuses on repayment hardship and servicing actions)
        WHEN sub_issue IN (
            'Struggling to repay your loan', 'Struggling to pay your loan',
            'Struggling to pay your bill', 'Struggling to pay mortgage',
            'Repaying your loan', 'Loan payment wasn''t credited to your account',
            'Loan servicing, payments, escrow account', 'Issues with repayment',
            'Loan modification,collection,foreclosure'
        ) THEN 'Loan Servicing & Modification'

        -- 3. Credit Reporting & Data Accuracy
        WHEN sub_issue IN (
            'Problem with credit report or credit score',
            'Unable to get your credit report or credit score',
            'Unable to get credit report/credit score',
            'Incorrect information on your report',
            'Incorrect information on credit report',
            'Credit reporting company''s investigation',
            'Problem with a credit reporting company''s investigation into an existing problem', -- Note: Long value
            'Problem with a company''s investigation into an existing problem',
            'Problem with a company''s investigation into an existing issue',
            'Improper use of your report', 'Improper use of my credit report'
        ) THEN 'Credit Reporting & Data Accuracy'

        -- 2. Fraud & Unauthorized Activity
        WHEN sub_issue IN (
            'Unauthorized withdrawals or charges',
            'Unauthorized transactions/trans. issues',
            'Unauthorized transactions or other transaction problem',
            'Fraud or scam',
            'Identity theft / Fraud / Embezzlement'
        ) THEN 'Fraud & Unauthorized Activity'

        -- 7. Customer Service & Support
        WHEN sub_issue IN (
            'Problem with customer service',
            'Customer service / Customer relations',
            'Customer service/Customer relations',
            'Communication tactics'
        ) THEN 'Customer Service & Support'

        -- 1. Account Management
        WHEN sub_issue IN (
            'Opening an account', 'Closing your account', 'Closing an account',
            'Managing an account', 'Managing, opening, or closing account',
            'Account terms and changes', 'Account opening, closing, or management'
        ) THEN 'Account Management'

        -- 5. Billing & Payment Issues (For making/receiving payments)
        WHEN sub_issue IN (
            'Payment to acct not credited', 'Problem when making payments',
            'Making/receiving payments, sending money', 'Trouble during payment process'
        ) THEN 'Billing & Payment Issues'

        -- 4. Fees & Charges (Overdrafts and NSFs are explicitly grouped under Fees in your scheme, but I'll make a separate category for clarity and use the most appropriate one for this list)
        WHEN sub_issue IN (
            'Problem with overdraft', 'Problem with an overdraft',
            'Overdraft, savings or rewards features', 'Overdraft, savings, or rewards features',
            'Problem caused by your funds being low', 'Problems caused by my funds being low' -- Note: Typo in original data
        ) THEN 'Fees & Charges (Overdraft/Balance)'

        -- 8. Loan/Product Terms & Disclosure
        WHEN sub_issue IN (
            'Confusing or misleading advertising or marketing',
            'False statements or representation',
            'Advertising and marketing, including promotional offers',
            'Advertising and marketing', 'Advertising',
            'Disclosures', 'Disclosure verification of debt',
            'Incorrect/missing disclosures or info'
        ) THEN 'Loan/Product Terms & Disclosure'

        -- 6. Debt Collection Practices
        WHEN sub_issue IN (
            'Collection practices', 'Cont''d attempts collect debt not owed',
            'Took or threatened to take negative or legal action',
            'Taking/threatening an illegal action',
            'Threatened to contact someone or share information improperly',
            'Improper contact or sharing of info'
        ) THEN 'Debt Collection Practices'

        -- Other Products & Services (Loan Lifecycle)
        WHEN sub_issue IN (
            'Managing the loan or lease', 'Problems at the end of the loan or lease',
            'Problem with the payoff process at the end of the loan',
            'Payoff process', 'Taking out the loan or lease'
        ) THEN 'Other Products & Services (Loan Lifecycle)'

        -- 17. Credit Limit & Arbitration (The best fit for Credit Decisions that change limits)
        WHEN sub_issue IN (
            'Credit determination', 'Credit decision / Underwriting',
            'Credit limit changed', 'Credit line increase/decrease'
        ) THEN 'Credit Limit & Arbitration'

        -- 13. Card Functionality & Usage
        WHEN sub_issue IN (
            'Trouble using your card', 'Trouble using the card',
            'Getting a credit card', 'Problem getting a card or closing an account',
            'Unsolicited issuance of credit card'
        ) THEN 'Card Functionality & Usage'

        -- 1. Account Management (Mobile Wallet is essentially a digital account)
        WHEN sub_issue IN (
            'Trouble accessing funds in your mobile or digital wallet',
            'Managing, opening, or closing your mobile wallet account'
        ) THEN 'Account Management (Digital)'

        -- Other Products & Services (Transfers)
        WHEN sub_issue IN (
            'Cash advance', 'Cash advance fee',
            'Money was taken from your bank account on the wrong day or for the wrong amount'
        ) THEN 'Other Products & Services (Transfers)'

        -- 12. Vehicle Loans & Repossession (Property/Vehicle damage/loss)
        WHEN sub_issue IN (
            'Vehicle was repossessed or sold the vehicle',
            'Vehicle was damaged or destroyed the vehicle',
            'Lender repossessed or sold the vehicle',
            'Lender damaged or destroyed vehicle',
            'Property was sold', 'Property was damaged or destroyed property',
            'Lender damaged or destroyed property'
        ) THEN 'Vehicle Loans & Repossession'

        -- 14. Privacy & Unwanted Marketing
        WHEN sub_issue IN (
            'Privacy', 'Identity theft protection or other monitoring services',
            'Credit monitoring or identity theft protection services',
            'Credit monitoring or identity protection',
            'Problem with fraud alerts or security freezes'
        ) THEN 'Privacy & Unwanted Marketing'

        -- 9. Loan Origination & Application
        WHEN sub_issue IN (
            'Application processing delay',
            'Application, originator, mortgage broker',
            'Applying for a mortgage or refinancing an existing mortgage'
        ) THEN 'Loan Origination & Application'

        -- 18. General/Miscellaneous
        WHEN sub_issue IN (
            'Other transaction problem', 'Other transaction issues',
            'Other service problem', 'Other service issues',
            'Other features, terms, or problems', 'Other'
        ) THEN 'General/Miscellaneous'

        -- Preserve original if no match, defaulting to 'General/Miscellaneous'
        ELSE 'General/Miscellaneous'
    END,
    -- 4. Clean 'company_public_response'
    company_public_response_standardized = CASE
        WHEN company_public_response IS NULL OR TRIM(company_public_response) = '' OR company_public_response = 'None' THEN 'N/A'
        WHEN company_public_response = 'Company believes complaint caused principally by actions of third party outside the control or direction of the company' THEN 'Third Party Responsibility'
        WHEN company_public_response = 'Company believes complaint is the result of an isolated error' THEN 'Isolated Error'
        WHEN company_public_response = 'Company believes complaint relates to a discontinued policy or procedure' THEN 'Discontinued Policy/Procedure'
        WHEN company_public_response = 'Company believes complaint represents an opportunity for improvement to better serve consumers' THEN 'Opportunity for Improvement'
        WHEN company_public_response = 'Company believes it acted appropriately as authorized by contract or law' THEN 'Acted per Contract/Law'
        WHEN company_public_response = 'Company believes the complaint is the result of a misunderstanding' THEN 'Result of Misunderstanding'
        WHEN company_public_response = 'Company believes the complaint provided an opportunity to answer consumer''s questions' THEN 'Consumer Question Answered'
        WHEN company_public_response = 'Company can''t verify or dispute the facts in the complaint' THEN 'Cannot Verify/Dispute Facts'
        WHEN company_public_response IN ('Company chooses not to provide a public response', 'Company has responded to the consumer and the CFPB and chooses not to provide a public response') THEN 'No Public Response'
        WHEN company_public_response = 'Company disputes the facts presented in the complaint' THEN 'Disputes Complaint Facts'
        ELSE company_public_response
    END,
    company_response_to_consumer_standardized = CASE
    -- 1. Handle Null/Empty values first, mapping to 'N/A'
    WHEN company_response_to_consumer IS NULL 
    OR TRIM(company_response_to_consumer) = '' 
    THEN 'N/A'

    -- 2. Map specific relief types
    WHEN company_response_to_consumer = 'Closed with monetary relief' 
    THEN 'Monetary Relief'
    
    WHEN company_response_to_consumer = 'Closed with non-monetary relief' 
    THEN 'Non-monetary Relief'

    -- 3. Map general/unspecified relief (this often overlaps with the above, but should be mapped clearly)
    WHEN company_response_to_consumer = 'Closed with relief' 
    THEN 'Unspecified Relief'
    
    -- 4. Map closure outcomes without relief
    WHEN company_response_to_consumer = 'Closed without relief' 
    THEN 'No Relief'

    -- 5. Map informational closure
    WHEN company_response_to_consumer = 'Closed with explanation' 
    THEN 'Explanation Provided'
    
    -- 6. Preserve other values (e.g., 'In progress', 'Closed')
    ELSE company_response_to_consumer 
    END,

    -- 7. Clean 'tags'
    tags_standardized = CASE
        WHEN tags IS NULL OR TRIM(tags) = '' THEN 'General'
        WHEN tags = 'Older American' THEN 'Older American'
        WHEN tags = 'Servicemember' THEN 'Servicemember'
        WHEN tags = 'Older American, Servicemember' THEN 'Older American & Servicemember'
        ELSE tags -- Preserve original if no match
    END,

    -- 8. Clean 'consumer_consent_provided'
    consumer_consent_provided_standardized = CASE
    -- 1. Handle Null, Empty, or N/A values first, mapping to 'N/A'
        WHEN consumer_consent_provided IS NULL 
        OR TRIM(consumer_consent_provided) = '' 
        OR consumer_consent_provided = 'N/A' 
        THEN 'N/A'
        
        -- 2. Standardize all forms of 'Provided'
        WHEN consumer_consent_provided = 'Consent provided' 
        THEN 'Provided'
        
        -- 3. Standardize 'Not provided'
        WHEN consumer_consent_provided = 'Consent not provided' 
        THEN 'Not provided'
        
        -- 4. Standardize 'Withdrawn'
        WHEN consumer_consent_provided = 'Consent withdrawn' 
        THEN 'Withdrawn'
        
        -- 5. Default to the existing value if it's already one of the clean values or an unexpected value
        ELSE consumer_consent_provided 
    END,

    -- 9. Clean 'consumer_disputed'
    consumer_disputed_standardized = IFNULL(NULLIF(TRIM(consumer_disputed), ''), 'N/A'),

    -- 10. Clean 'consumer_complaint_narrative'
    consumer_complaint_narrative = IFNULL(NULLIF(TRIM(consumer_complaint_narrative), ''), 'None'),

    -- 13. Clean 'product'
    product_standardized = CASE
        -- 1. Handle Null/Blank Values
        WHEN product IS NULL OR TRIM(product) = '' THEN 'N/A'
        
        -- 2. Deposit Accounts: Keep Checking/Savings distinct from general services
        WHEN product IN (
            'Checking or savings account'
        ) THEN 'Deposit Account (Checking/Savings)'
        
        -- 3. General Banking Services
        WHEN product IN (
            'Bank account or service'
        ) THEN 'General Bank Service'
        
        -- 4. Credit Products (Revolving): Credit Card only
        WHEN product IN (
            'Credit card', 
            'Credit card or prepaid card'
        ) THEN 'Credit Card'
        
        -- 5. Prepaid Card
        WHEN product IN (
            'Prepaid card'
        ) THEN 'Prepaid Card'
        
        -- 6. Credit Reporting/Repair: Keep all related services together
        WHEN product IN (
            'Credit reporting',
            'Credit reporting or other personal consumer reports',
            'Credit reporting, credit repair services, or other personal consumer reports'
        ) THEN 'Credit Reporting/Repair Service'
        
        -- 7. Debt Management: Debt collection, counseling, etc.
        WHEN product IN (
            'Debt collection', 
            'Debt or credit management'
        ) THEN 'Debt Collection/Management'
        
        -- 8. Money Transfers: Isolating standard money movement
        WHEN product IN (
            'Money transfer, virtual currency, or money service',
            'Money transfers'
        ) THEN 'Money Transfer Service'

        -- 9. Virtual Currency: Giving virtual currency its own category for tracking emerging issues
        WHEN product IN (
            'Virtual currency'
        ) THEN 'Virtual Currency'
        
        -- 10. Mortgages
        WHEN product = 'Mortgage' THEN 'Mortgage'
        
        -- 11. Short-Term Loans: Payday, Title, and Advance Loans
        WHEN product IN (
            'Payday loan',
            'Payday loan, title loan, or personal loan',
            'Payday loan, title loan, personal loan, or advance loan'
        ) THEN 'Payday/Title/Advance Loan'
        
        -- 12. Installment Loans (Specific): Student Loan
        WHEN product = 'Student loan' THEN 'Student Loan'
        
        -- 13. Installment Loans (Specific): Vehicle Loan/Lease
        WHEN product = 'Vehicle loan or lease' THEN 'Vehicle Loan/Lease'
        
        -- 14. Installment Loans (General): Personal Loan
        WHEN product IN ('Consumer Loan') THEN 'Personal Loan'

        -- 15. Other/Miscellaneous
        WHEN product = 'Other financial service' THEN 'Other Financial Service'
        
        -- Preserve original if no match (will be caught by 'Other' if not mapped above)
        ELSE product 
    END,

    -- 14. Clean 'sub_product'
    sub_product_standardized = CASE
        -- 1. Handle Null/Empty/Generic Values First
        WHEN sub_product IS NULL OR TRIM(sub_product) = '' THEN 'N/A'
        WHEN sub_product = 'I do not know' THEN 'Unknown'
        WHEN sub_product = 'Other (i.e. phone, health club, etc.)' THEN 'Other Non-Financial'
        WHEN sub_product = 'Other debt' THEN 'Other Debt'
        WHEN sub_product = 'Other advances of future income' THEN 'Other Advance'
        WHEN sub_product IN ('Other bank product/service', 'Other banking product or service') THEN 'Other Bank Product'

        -- 2. Credit Cards (Group all types)
        WHEN sub_product IN (
            'Credit card', 'Store credit card', 'General-purpose credit card or charge card', 
            'Credit Card Debt'
        ) THEN 'Credit Card'

        -- 3. Short-Term/Specialty Loans
        WHEN sub_product IN ('Payday loan', 'Payday loan debt') THEN 'Payday Loan'
        WHEN sub_product = 'Title loan' THEN 'Title Loan'
        WHEN sub_product = 'Pawn loan' THEN 'Pawn Loan'
        WHEN sub_product = 'Earned wage access' THEN 'Wage Access'

        -- 4. General Loans (Keep Personal/Installment/Line of Credit separate)
        WHEN sub_product = 'Personal line of credit' THEN 'Personal Line of Credit'
        WHEN sub_product = 'Installment loan' THEN 'Installment Loan'
        WHEN sub_product = 'Loan' THEN 'General Loan'

        -- 5. Vehicle Loans/Leases
        WHEN sub_product IN ('Vehicle loan', 'Auto', 'Auto debt', 'Vehicle lease') THEN 'Vehicle Loan/Lease'
        
        -- 6. Mortgages (Group similar conventional types)
        WHEN sub_product IN (
            'Mortgage', 'Mortgage debt', 'Other mortgage', 'Other type of mortgage',
            'Conventional home mortgage', 'Conventional fixed mortgage', 'Conventional adjustable mortgage (ARM)'
        ) THEN 'General/Conventional Mortgage'
        WHEN sub_product = 'FHA mortgage' THEN 'FHA Mortgage'
        WHEN sub_product = 'VA mortgage' THEN 'VA Mortgage'
        WHEN sub_product = 'USDA mortgage' THEN 'USDA Mortgage'
        WHEN sub_product = 'Reverse mortgage' THEN 'Reverse Mortgage'
        WHEN sub_product = 'Second mortgage' THEN 'Second Mortgage'
        WHEN sub_product IN ('Home equity loan or line of credit', 'Home equity loan or line of credit (HELOC)') THEN 'HELOC'
        WHEN sub_product = 'Manufactured home loan' THEN 'Manufactured Home Loan'
        WHEN sub_product = 'Mortgage modification or foreclosure avoidance' THEN 'Mortgage Assistance'

        -- 7. Student Loans
        WHEN sub_product IN ('Federal student loan', 'Federal student loan servicing', 'Federal student loan debt') THEN 'Federal Student Loan'
        WHEN sub_product IN ('Private student loan', 'Non-federal student loan', 'Private student loan debt') THEN 'Private/Non-Federal Student Loan'
        WHEN sub_product = 'Student loan debt relief' THEN 'Student Loan Relief'

        -- 8. Money Movement Services
        WHEN sub_product IN ('Domestic (US) money transfer', 'International money transfer', 'Money Transfer') THEN 'Money Transfer'
        WHEN sub_product IN ('Money order', 'Money order, traveler''s check or cashier''s check', 'Traveler''s check or cashier''s check', 'Traveler’s/Cashier’s checks') THEN 'Money Instrument (Check)'
        WHEN sub_product IN ('Foreign currency exchange', 'Currency Exchange') THEN 'Currency Exchange'
        WHEN sub_product = 'Virtual currency' THEN 'Virtual Currency'
        WHEN sub_product = 'Cashing a check without an account' THEN 'Check Cashing'
        WHEN sub_product IN ('Mobile wallet', 'Mobile or digital wallet') THEN 'Mobile Wallet'

        -- 9. Debt Collection Types
        WHEN sub_product = 'Rental debt' THEN 'Rental Debt'
        WHEN sub_product IN ('Medical debt', 'Medical') THEN 'Medical Debt'
        WHEN sub_product = 'Telecommunications debt' THEN 'Telecom Debt'
        WHEN sub_product = 'Debt settlement' THEN 'Debt Settlement'

        -- 10. Credit Reporting & Repair Services
        WHEN sub_product = 'Credit reporting' THEN 'Credit Reporting'
        WHEN sub_product = 'Other personal consumer report' THEN 'Other Consumer Report'
        WHEN sub_product = 'Credit repair services' THEN 'Credit Repair Service'

        -- 11. Cards (Other than Credit)
        WHEN sub_product IN ('Gift card', 'Gift or merchant card') THEN 'Gift Card'
        WHEN sub_product IN ('General-purpose prepaid card', 'General purpose card', 'Student prepaid card', 'ID prepaid card') THEN 'General Prepaid Card'
        WHEN sub_product = 'Transit card' THEN 'Transit Card'
        WHEN sub_product = 'Payroll card' THEN 'Payroll Card'
        WHEN sub_product IN ('Government benefit card', 'Government benefit payment card', 'Electronic Benefit Transfer / EBT card') THEN 'Government Benefit Card'

        -- 12. Bank Account Products
        WHEN sub_product IN ('CD (Certificate of Deposit)', '(CD) Certificate of deposit') THEN 'Certificate of Deposit'
        WHEN sub_product IN ('Refund anticipation check', 'Tax refund anticipation loan or check') THEN 'Tax Refund Product/Advance'

        -- Preserve original if no match (should only happen if new, unmapped values appear)
        ELSE sub_product 
    END

-- The WHERE clause is simple because we are updating the entire staging table.
WHERE 1=1 {limit_clause};
