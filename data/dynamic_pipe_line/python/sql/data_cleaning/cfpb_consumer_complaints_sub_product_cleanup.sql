-- Standardizes 'Sub-product' by setting empty strings to 'N/A' and mapping to a refined, consistent set of categories.
UPDATE consumer_complaints_staging
SET sub_product = CASE
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
WHERE 1=1
{limit_clause};