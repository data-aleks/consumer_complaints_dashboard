-- Standardize Credit Card
UPDATE consumer_complaints
SET sub_product = 'Credit Card'
WHERE sub_product IN (
  'Credit card',
  'Store credit card',
  'General-purpose credit card or charge card'
);

-- Standardize Vehicle Loan
UPDATE consumer_complaints
SET sub_product = 'Vehicle Loan'
WHERE sub_product IN (
  'Vehicle loan',
  'Auto',
  'Auto debt',
  'Vehicle lease'
);

-- Standardize Payday Loan
UPDATE consumer_complaints
SET sub_product = 'Payday Loan'
WHERE sub_product IN (
  'Payday loan',
  'Payday loan debt'
);

-- Standardize Title Loan
UPDATE consumer_complaints
SET sub_product = 'Title Loan'
WHERE sub_product = 'Title loan';

-- Standardize Personal Line of Credit
UPDATE consumer_complaints
SET sub_product = 'Personal Line of Credit'
WHERE sub_product = 'Personal line of credit';

-- Standardize Installment Loan
UPDATE consumer_complaints
SET sub_product = 'Installment Loan'
WHERE sub_product = 'Installment loan';

-- Standardize Loan
UPDATE consumer_complaints
SET sub_product = 'Loan'
WHERE sub_product = 'Loan';

-- Standardize Mortgage
UPDATE consumer_complaints
SET sub_product = 'Mortgage'
WHERE sub_product IN (
  'Mortgage',
  'Mortgage debt',
  'Other mortgage',
  'Other type of mortgage'
);

-- Standardize Conventional Mortgage
UPDATE consumer_complaints
SET sub_product = 'Conventional Mortgage'
WHERE sub_product IN (
  'Conventional home mortgage',
  'Conventional fixed mortgage',
  'Conventional adjustable mortgage (ARM)'
);

-- Standardize FHA Mortgage
UPDATE consumer_complaints
SET sub_product = 'FHA Mortgage'
WHERE sub_product = 'FHA mortgage';

-- Standardize VA Mortgage
UPDATE consumer_complaints
SET sub_product = 'VA Mortgage'
WHERE sub_product = 'VA mortgage';

-- Standardize USDA Mortgage
UPDATE consumer_complaints
SET sub_product = 'USDA Mortgage'
WHERE sub_product = 'USDA mortgage';

-- Standardize Reverse Mortgage
UPDATE consumer_complaints
SET sub_product = 'Reverse Mortgage'
WHERE sub_product = 'Reverse mortgage';

-- Standardize Second Mortgage
UPDATE consumer_complaints
SET sub_product = 'Second Mortgage'
WHERE sub_product = 'Second mortgage';

-- Standardize HELOC
UPDATE consumer_complaints
SET sub_product = 'HELOC'
WHERE sub_product IN (
  'Home equity loan or line of credit',
  'Home equity loan or line of credit (HELOC)'
);

-- Standardize Manufactured Home Loan
UPDATE consumer_complaints
SET sub_product = 'Manufactured Home Loan'
WHERE sub_product = 'Manufactured home loan';

-- Standardize Mortgage Assistance
UPDATE consumer_complaints
SET sub_product = 'Mortgage Assistance'
WHERE sub_product = 'Mortgage modification or foreclosure avoidance';

-- Standardize Federal Student Loan
UPDATE consumer_complaints
SET sub_product = 'Federal Student Loan'
WHERE sub_product IN (
  'Federal student loan',
  'Federal student loan servicing',
  'Federal student loan debt'
);

-- Standardize Private Student Loan
UPDATE consumer_complaints
SET sub_product = 'Private Student Loan'
WHERE sub_product IN (
  'Private student loan',
  'Private student loan debt'
);

-- Standardize Non-Federal Student Loan
UPDATE consumer_complaints
SET sub_product = 'Non-Federal Student Loan'
WHERE sub_product = 'Non-federal student loan';

-- Standardize Student Loan Relief
UPDATE consumer_complaints
SET sub_product = 'Student Loan Relief'
WHERE sub_product = 'Student loan debt relief';

-- Standardize Money Transfer
UPDATE consumer_complaints
SET sub_product = 'Money Transfer'
WHERE sub_product IN (
  'Domestic (US) money transfer',
  'International money transfer'
);

-- Standardize Money Instrument
UPDATE consumer_complaints
SET sub_product = 'Money Instrument'
WHERE sub_product IN (
  'Money order',
  'Money order, traveler\'s check or cashier\'s check'
);

-- Standardize Currency Exchange
UPDATE consumer_complaints
SET sub_product = 'Currency Exchange'
WHERE sub_product = 'Foreign currency exchange';

-- Standardize Virtual Currency
UPDATE consumer_complaints
SET sub_product = 'Virtual Currency'
WHERE sub_product = 'Virtual currency';

-- Standardize Credit Card Debt
UPDATE consumer_complaints
SET sub_product = 'Credit Card Debt'
WHERE sub_product = 'Credit card debt';

-- Standardize Rental Debt
UPDATE consumer_complaints
SET sub_product = 'Rental Debt'
WHERE sub_product = 'Rental debt';

-- Standardize Medical Debt
UPDATE consumer_complaints
SET sub_product = 'Medical Debt'
WHERE sub_product IN (
  'Medical debt',
  'Medical'
);

-- Standardize Telecom Debt
UPDATE consumer_complaints
SET sub_product = 'Telecom Debt'
WHERE sub_product = 'Telecommunications debt';

-- Standardize Other Debt
UPDATE consumer_complaints
SET sub_product = 'Other Debt'
WHERE sub_product = 'Other debt';

-- Standardize Debt Settlement
UPDATE consumer_complaints
SET sub_product = 'Debt Settlement'
WHERE sub_product = 'Debt settlement';

-- Standardize Credit Reporting
UPDATE consumer_complaints
SET sub_product = 'Credit Reporting'
WHERE sub_product = 'Credit reporting';

-- Standardize Other Consumer Report
UPDATE consumer_complaints
SET sub_product = 'Other Consumer Report'
WHERE sub_product = 'Other personal consumer report';

-- Standardize Credit Repair
UPDATE consumer_complaints
SET sub_product = 'Credit Repair'
WHERE sub_product = 'Credit repair services';

-- Standardize Check Cashing
UPDATE consumer_complaints
SET sub_product = 'Check Cashing'
WHERE sub_product = 'Cashing a check without an account';

-- Standardize Tax Refund Advance
UPDATE consumer_complaints
SET sub_product = 'Tax Refund Advance'
WHERE sub_product = 'Refund anticipation check';

-- Standardize Gift Card
UPDATE consumer_complaints
SET sub_product = 'Gift Card'
WHERE sub_product IN (
  'Gift card',
  'Gift or merchant card'
);

-- Standardize Prepaid Card
UPDATE consumer_complaints
SET sub_product = 'Prepaid Card'
WHERE sub_product IN (
  'General-purpose prepaid card',
  'General purpose card'
);

-- Standardize Mobile Wallet
UPDATE consumer_complaints
SET sub_product = 'Mobile Wallet'
WHERE sub_product IN (
  'Mobile wallet',
  'Mobile or digital wallet'
);

-- Standardize Transit Card
UPDATE consumer_complaints
SET sub_product = 'Transit Card'
WHERE sub_product = 'Transit card';

-- Standardize Payroll Card
UPDATE consumer_complaints
SET sub_product = 'Payroll Card'
WHERE sub_product = 'Payroll card';

-- Standardize Government Benefit Card
UPDATE consumer_complaints
SET sub_product = 'Government Benefit Card'
WHERE sub_product IN (
  'Government benefit card',
  'Government benefit payment card'
);

-- Standardize Certificate of Deposit
UPDATE consumer_complaints
SET sub_product = 'Certificate of Deposit'
WHERE sub_product IN (
  'CD (Certificate of Deposit)',
  '(CD) Certificate of deposit'
);

-- Standardize Other Bank Product
UPDATE consumer_complaints
SET sub_product = 'Other Bank Product'
WHERE sub_product IN (
  'Other bank product/service',
  'Other banking product or service'
);

-- Standardize Wage Access
UPDATE consumer_complaints
SET sub_product = 'Wage Access'
WHERE sub_product = 'Earned wage access';

-- Standardize Other Advance
UPDATE consumer_complaints
SET sub_product = 'Other Advance'
WHERE sub_product = 'Other advances of future income';

-- Standardize Other
UPDATE consumer_complaints
SET sub_product = 'Other'
WHERE sub_product = 'Other (i.e. phone, health club, etc.)';

-- Standardize Unknown
UPDATE consumer_complaints
SET sub_product = 'Unknown'
WHERE sub_product IN ('', 'I do not know');