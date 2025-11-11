-- Standardizes 'Sub-product' by setting empty strings to NULL.
-- Standardizes 'Sub-product' values into a consistent set of categories.
UPDATE consumer_complaints_raw
SET sub_product = CASE
    -- Handle null/empty values
    WHEN sub_product IS NULL OR TRIM(sub_product) = '' OR sub_product = 'I do not know' THEN 'Unknown'
    WHEN sub_product = 'Other (i.e. phone, health club, etc.)' THEN 'Other'

    -- Credit Card
    WHEN sub_product IN ('Credit card', 'Store credit card', 'General-purpose credit card or charge card') THEN 'Credit Card'

    -- Loans
    WHEN sub_product IN ('Vehicle loan', 'Auto', 'Auto debt', 'Vehicle lease') THEN 'Vehicle Loan'
    WHEN sub_product IN ('Payday loan', 'Payday loan debt') THEN 'Payday Loan'
    WHEN sub_product = 'Title loan' THEN 'Title Loan'
    WHEN sub_product = 'Personal line of credit' THEN 'Personal Line of Credit'
    WHEN sub_product = 'Installment loan' THEN 'Installment Loan'
    WHEN sub_product = 'Loan' THEN 'Loan'

    -- Mortgages
    WHEN sub_product IN ('Mortgage', 'Mortgage debt', 'Other mortgage', 'Other type of mortgage') THEN 'Mortgage'
    WHEN sub_product IN ('Conventional home mortgage', 'Conventional fixed mortgage', 'Conventional adjustable mortgage (ARM)') THEN 'Conventional Mortgage'
    WHEN sub_product = 'FHA mortgage' THEN 'FHA Mortgage'
    WHEN sub_product = 'VA mortgage' THEN 'VA Mortgage'
    WHEN sub_product = 'USDA mortgage' THEN 'USDA Mortgage'
    WHEN sub_product = 'Reverse mortgage' THEN 'Reverse Mortgage'
    WHEN sub_product = 'Second mortgage' THEN 'Second Mortgage'
    WHEN sub_product IN ('Home equity loan or line of credit', 'Home equity loan or line of credit (HELOC)') THEN 'HELOC'
    WHEN sub_product = 'Manufactured home loan' THEN 'Manufactured Home Loan'
    WHEN sub_product = 'Mortgage modification or foreclosure avoidance' THEN 'Mortgage Assistance'

    -- Student Loans
    WHEN sub_product IN ('Federal student loan', 'Federal student loan servicing', 'Federal student loan debt') THEN 'Federal Student Loan'
    WHEN sub_product IN ('Private student loan', 'Private student loan debt') THEN 'Private Student Loan'
    WHEN sub_product = 'Non-federal student loan' THEN 'Non-Federal Student Loan'
    WHEN sub_product = 'Student loan debt relief' THEN 'Student Loan Relief'

    -- Money Services
    WHEN sub_product IN ('Domestic (US) money transfer', 'International money transfer') THEN 'Money Transfer'
    WHEN sub_product IN ('Money order', 'Money order, traveler''s check or cashier''s check') THEN 'Money Instrument'
    WHEN sub_product = 'Foreign currency exchange' THEN 'Currency Exchange'
    WHEN sub_product = 'Virtual currency' THEN 'Virtual Currency'
    WHEN sub_product = 'Cashing a check without an account' THEN 'Check Cashing'

    -- Debt Collection
    WHEN sub_product = 'Credit card debt' THEN 'Credit Card Debt'
    WHEN sub_product = 'Rental debt' THEN 'Rental Debt'
    WHEN sub_product IN ('Medical debt', 'Medical') THEN 'Medical Debt'
    WHEN sub_product = 'Telecommunications debt' THEN 'Telecom Debt'
    WHEN sub_product = 'Other debt' THEN 'Other Debt'
    WHEN sub_product = 'Debt settlement' THEN 'Debt Settlement'

    -- Credit Reporting & Repair
    WHEN sub_product = 'Credit reporting' THEN 'Credit Reporting'
    WHEN sub_product = 'Other personal consumer report' THEN 'Other Consumer Report'
    WHEN sub_product = 'Credit repair services' THEN 'Credit Repair'

    -- Cards & Wallets
    WHEN sub_product IN ('Gift card', 'Gift or merchant card') THEN 'Gift Card'
    WHEN sub_product IN ('General-purpose prepaid card', 'General purpose card') THEN 'Prepaid Card'
    WHEN sub_product IN ('Mobile wallet', 'Mobile or digital wallet') THEN 'Mobile Wallet'
    WHEN sub_product = 'Transit card' THEN 'Transit Card'
    WHEN sub_product = 'Payroll card' THEN 'Payroll Card'
    WHEN sub_product IN ('Government benefit card', 'Government benefit payment card') THEN 'Government Benefit Card'

    -- Bank Products
    WHEN sub_product IN ('CD (Certificate of Deposit)', '(CD) Certificate of deposit') THEN 'Certificate of Deposit'
    WHEN sub_product IN ('Other bank product/service', 'Other banking product or service') THEN 'Other Bank Product'
    WHEN sub_product = 'Refund anticipation check' THEN 'Tax Refund Advance'
    WHEN sub_product = 'Earned wage access' THEN 'Wage Access'
    WHEN sub_product = 'Other advances of future income' THEN 'Other Advance'

    ELSE sub_product -- Keep the original value if no rule matches
END
WHERE sub_product IS NOT NULL {incremental_clause} {limit_clause};