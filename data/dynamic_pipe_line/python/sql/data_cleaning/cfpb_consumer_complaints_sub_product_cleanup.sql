-- CFPB Consumer Complaints Cleanup: sub_product
-- Standardizes sub_product values in the cleaned table

-- Credit Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Credit Card'
WHERE sub_product IN (
  'Credit card',
  'Store credit card',
  'General-purpose credit card or charge card'
);

-- Vehicle Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Vehicle Loan'
WHERE sub_product IN (
  'Vehicle loan',
  'Auto',
  'Auto debt',
  'Vehicle lease'
);

-- Payday Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Payday Loan'
WHERE sub_product IN (
  'Payday loan',
  'Payday loan debt'
);

-- Title Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Title Loan'
WHERE sub_product = 'Title loan';

-- Personal Line of Credit
UPDATE consumer_complaints_cleaned
SET sub_product = 'Personal Line of Credit'
WHERE sub_product = 'Personal line of credit';

-- Installment Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Installment Loan'
WHERE sub_product = 'Installment loan';

-- Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Loan'
WHERE sub_product = 'Loan';

-- Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'Mortgage'
WHERE sub_product IN (
  'Mortgage',
  'Mortgage debt',
  'Other mortgage',
  'Other type of mortgage'
);

-- Conventional Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'Conventional Mortgage'
WHERE sub_product IN (
  'Conventional home mortgage',
  'Conventional fixed mortgage',
  'Conventional adjustable mortgage (ARM)'
);

-- FHA Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'FHA Mortgage'
WHERE sub_product = 'FHA mortgage';

-- VA Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'VA Mortgage'
WHERE sub_product = 'VA mortgage';

-- USDA Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'USDA Mortgage'
WHERE sub_product = 'USDA mortgage';

-- Reverse Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'Reverse Mortgage'
WHERE sub_product = 'Reverse mortgage';

-- Second Mortgage
UPDATE consumer_complaints_cleaned
SET sub_product = 'Second Mortgage'
WHERE sub_product = 'Second mortgage';

-- HELOC
UPDATE consumer_complaints_cleaned
SET sub_product = 'HELOC'
WHERE sub_product IN (
  'Home equity loan or line of credit',
  'Home equity loan or line of credit (HELOC)'
);

-- Manufactured Home Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Manufactured Home Loan'
WHERE sub_product = 'Manufactured home loan';

-- Mortgage Assistance
UPDATE consumer_complaints_cleaned
SET sub_product = 'Mortgage Assistance'
WHERE sub_product = 'Mortgage modification or foreclosure avoidance';

-- Federal Student Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Federal Student Loan'
WHERE sub_product IN (
  'Federal student loan',
  'Federal student loan servicing',
  'Federal student loan debt'
);

-- Private Student Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Private Student Loan'
WHERE sub_product IN (
  'Private student loan',
  'Private student loan debt'
);

-- Non-Federal Student Loan
UPDATE consumer_complaints_cleaned
SET sub_product = 'Non-Federal Student Loan'
WHERE sub_product = 'Non-federal student loan';

-- Student Loan Relief
UPDATE consumer_complaints_cleaned
SET sub_product = 'Student Loan Relief'
WHERE sub_product = 'Student loan debt relief';

-- Money Transfer
UPDATE consumer_complaints_cleaned
SET sub_product = 'Money Transfer'
WHERE sub_product IN (
  'Domestic (US) money transfer',
  'International money transfer'
);

-- Money Instrument
UPDATE consumer_complaints_cleaned
SET sub_product = 'Money Instrument'
WHERE sub_product IN (
  'Money order',
  'Money order, traveler\'s check or cashier\'s check'
);

-- Currency Exchange
UPDATE consumer_complaints_cleaned
SET sub_product = 'Currency Exchange'
WHERE sub_product = 'Foreign currency exchange';

-- Virtual Currency
UPDATE consumer_complaints_cleaned
SET sub_product = 'Virtual Currency'
WHERE sub_product = 'Virtual currency';

-- Credit Card Debt
UPDATE consumer_complaints_cleaned
SET sub_product = 'Credit Card Debt'
WHERE sub_product = 'Credit card debt';

-- Rental Debt
UPDATE consumer_complaints_cleaned
SET sub_product = 'Rental Debt'
WHERE sub_product = 'Rental debt';

-- Medical Debt
UPDATE consumer_complaints_cleaned
SET sub_product = 'Medical Debt'
WHERE sub_product IN (
  'Medical debt',
  'Medical'
);

-- Telecom Debt
UPDATE consumer_complaints_cleaned
SET sub_product = 'Telecom Debt'
WHERE sub_product = 'Telecommunications debt';

-- Other Debt
UPDATE consumer_complaints_cleaned
SET sub_product = 'Other Debt'
WHERE sub_product = 'Other debt';

-- Debt Settlement
UPDATE consumer_complaints_cleaned
SET sub_product = 'Debt Settlement'
WHERE sub_product = 'Debt settlement';

-- Credit Reporting
UPDATE consumer_complaints_cleaned
SET sub_product = 'Credit Reporting'
WHERE sub_product = 'Credit reporting';

-- Other Consumer Report
UPDATE consumer_complaints_cleaned
SET sub_product = 'Other Consumer Report'
WHERE sub_product = 'Other personal consumer report';

-- Credit Repair
UPDATE consumer_complaints_cleaned
SET sub_product = 'Credit Repair'
WHERE sub_product = 'Credit repair services';

-- Check Cashing
UPDATE consumer_complaints_cleaned
SET sub_product = 'Check Cashing'
WHERE sub_product = 'Cashing a check without an account';

-- Tax Refund Advance
UPDATE consumer_complaints_cleaned
SET sub_product = 'Tax Refund Advance'
WHERE sub_product = 'Refund anticipation check';

-- Gift Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Gift Card'
WHERE sub_product IN (
  'Gift card',
  'Gift or merchant card'
);

-- Prepaid Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Prepaid Card'
WHERE sub_product IN (
  'General-purpose prepaid card',
  'General purpose card'
);

-- Mobile Wallet
UPDATE consumer_complaints_cleaned
SET sub_product = 'Mobile Wallet'
WHERE sub_product IN (
  'Mobile wallet',
  'Mobile or digital wallet'
);

-- Transit Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Transit Card'
WHERE sub_product = 'Transit card';

-- Payroll Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Payroll Card'
WHERE sub_product = 'Payroll card';

-- Government Benefit Card
UPDATE consumer_complaints_cleaned
SET sub_product = 'Government Benefit Card'
WHERE sub_product IN (
  'Government benefit card',
  'Government benefit payment card'
);

-- Certificate of Deposit
UPDATE consumer_complaints_cleaned
SET sub_product = 'Certificate of Deposit'
WHERE sub_product IN (
  'CD (Certificate of Deposit)',
  '(CD) Certificate of deposit'
);

-- Other Bank Product
UPDATE consumer_complaints_cleaned
SET sub_product = 'Other Bank Product'
WHERE sub_product IN (
  'Other bank product/service',
  'Other banking product or service'
);

-- Wage Access
UPDATE consumer_complaints_cleaned
SET sub_product = 'Wage Access'
WHERE sub_product = 'Earned wage access';

-- Other Advance
UPDATE consumer_complaints_cleaned
SET sub_product = 'Other Advance'
WHERE sub_product = 'Other advances of future income';

-- Other
UPDATE consumer_complaints_cleaned
SET sub_product = 'Other'
WHERE sub_product = 'Other (i.e. phone, health club, etc.)';

-- Unknown
UPDATE consumer_complaints_cleaned
SET sub_product = 'Unknown'
WHERE sub_product IN ('', 'I do not know');