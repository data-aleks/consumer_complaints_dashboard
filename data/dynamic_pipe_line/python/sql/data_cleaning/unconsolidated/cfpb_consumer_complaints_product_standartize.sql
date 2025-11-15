-- Standardizes 'Product' by setting empty strings to 'N/A' and mapping to granular categories
UPDATE consumer_complaints_staging
SET product = CASE
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
END
WHERE 1=1
{limit_clause};