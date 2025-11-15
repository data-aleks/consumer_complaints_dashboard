UPDATE consumer_complaints_staging
SET issue = CASE
    -- Credit Reporting: Focusing ONLY on reporting/investigation
    WHEN issue IN (
        'Incorrect information on your report', 
        'Incorrect information on credit report', 
        'Problem with a company\'s investigation into an existing problem', 
        'Problem with a credit reporting company\'s investigation into an existing problem', 
        'Credit reporting company\'s investigation', 
        'Problem with a company\'s investigation into an existing issue', 
        'Improper use of your report', 
        'Improper use of my credit report', 
        'Problem with a purchase shown on your statement'
    ) THEN 'Credit Reporting & Investigation'

    -- Fraud & ID Theft: Isolating the fraud-related complaints from the old Credit Reporting bucket
    WHEN issue IN (
        'Credit Reporting, Fraud, or Identity Theft'
    ) THEN 'Fraud & Identity Theft'

    -- Debt Collection: Separating communication/legal issues from collection tactics
    WHEN issue IN (
        'Attempts to collect debt not owed', 
        'Cont\'d attempts collect debt not owed', 
        'False statements or representation', 
        'Written notification about debt', 
        'Disclosure verification of debt'
    ) THEN 'Debt Validity & Disputed Debt'
    
    -- Collection Communication & Tactics: Focusing on the method of collection
    WHEN issue IN (
        'Communication tactics', 
        'Electronic communications', 
        'Threatened to contact someone or share information improperly', 
        'Taking/threatening an illegal action'
    ) THEN 'Collection Communication & Tactics'

    -- Loan Hardship & Repayment: Keeping all payment difficulty issues together
    WHEN issue IN (
        'Struggling to pay mortgage', 
        'Struggling to repay your loan', 
        'Can\'t repay my loan', 
        'Struggling to pay your loan', 
        'Struggling to pay your bill', 
        'Struggling to repay your loan'
    ) THEN 'Loan Hardship & Repayment'

    -- Account/Card Management: Combining account access and status issues
    WHEN issue IN (
        'Managing an account', 
        'Opening an account', 
        'Closing your account', 
        'Closing an account', 
        'Problem getting a card or closing an account', 
        'Problem accessing account', 
        'Problem with a purchase or transfer'
    ) THEN 'Account/Card Management & Access'

    -- Loan Origination & Closing: Focusing only on the application and finalization stage
    WHEN issue IN (
        'Getting a loan', 
        'Applying for a mortgage or refinancing an existing mortgage', 
        'Closing on a mortgage', 
        'Getting a loan or lease', 
        'Getting a credit card'
    ) THEN 'Loan Origination & Application'

    -- Fees & Overdrafts: Separating charges from disclosure/terms
    WHEN issue IN (
        'Fees or interest', 
        'Problem caused by your funds being low', 
        'Problem with overdraft'
    ) THEN 'Fees, Interest & Overdraft'

    -- Disclosure & Marketing: Separating these from Customer Service
    WHEN issue IN (
        'Advertising and marketing, including promotional offers', 
        'Advertising & Disclosure', 
        'Advertising'
    ) THEN 'Disclosure & Marketing'
    
    -- Customer Service & Servicer Contact: Focusing on contact quality and support
    WHEN issue IN (
        'Customer Service & Communication', 
        'Dealing with your lender or servicer', 
        'Dealing with my lender or servicer'
    ) THEN 'Customer Service & Servicing Contact'

    -- Transactional/Card Functionality: Focusing on use, payments, and transactions
    WHEN issue IN (
        'Trouble using the card', 
        'Trouble using your card', 
        'Transaction & Payment Issues', 
        'Trouble during payment process', 
        'Problem when making payments', 
        'Problem with a lender or other company charging your account'
    ) THEN 'Transactional & Card Functionality'

    -- End-of-Loan Lifecycle: Keeping repossession and lease end issues together
    WHEN issue IN (
        'Repossession', 
        'Problems at the end of the loan or lease', 
        'Managing the loan or lease'
    ) THEN 'End-of-Loan Lifecycle'

    -- Handle N/A and unmapped values
    WHEN issue IS NULL OR TRIM(issue) = '' THEN 'N/A'
    ELSE 'Other/Miscellaneous'
END
-- The WHERE clause remains simple to ensure all rows are processed
WHERE 1=1
    {limit_clause};