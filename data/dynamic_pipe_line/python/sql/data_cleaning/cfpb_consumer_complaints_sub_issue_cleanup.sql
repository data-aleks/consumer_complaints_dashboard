-- Standardizes 'Sub-issue' by setting empty strings/unknowns to 'Not available' and mapping known values
UPDATE consumer_complaints_staging
SET sub_issue = CASE
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

    -- 15. Other Products & Services (Loan Disbursement is often an issue with fund handling or processing)
    WHEN sub_issue IN (
        'Was approved for a loan, but didn''t receive the money',
        'Was approved for a loan, but didn''t receive money',
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
        'Problem with a credit reporting company''s investigation into an existing problem',
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
        'Problem caused by your funds being low', 'Problems caused by my funds being low'
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

    -- 15. Other Products & Services (Loan Lifecycle is too generic, better mapped to a product issue or origination/servicing)
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

    -- 15. Other Products & Services (General issues related to money movement/products)
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
END
WHERE 1=1
{limit_clause};