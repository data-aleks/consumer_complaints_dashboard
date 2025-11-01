-- CFPB Consumer Complaints Schema Cleanup: Product Column
-- Standardizes product categories

-- Payday/Title/Personal loan variants
UPDATE consumer_complaints
SET product = 'Payday/Title/Personal loan'
WHERE product IN (
  'Payday loan',
  'Payday loan, title loan, or personal loan',
  'Payday loan, title loan, personal loan, or advance loan'
);

-- Credit reporting variants
UPDATE consumer_complaints
SET product = 'Credit reporting'
WHERE product IN (
  'Credit reporting',
  'Credit reporting or other personal consumer reports',
  'Credit reporting, credit repair services, or other personal consumer reports'
);

-- Credit card variants
UPDATE consumer_complaints
SET product = 'Credit card'
WHERE product IN (
  'Credit card',
  'Credit card or prepaid card'
);

-- Money transfer variants
UPDATE consumer_complaints
SET product = 'Money transfer'
WHERE product IN (
  'Money transfers',
  'Money transfer, virtual currency, or money service'
);

-- Bank account variants
UPDATE consumer_complaints
SET product = 'Bank account'
WHERE product IN (
  'Checking or savings account',
  'Bank account or service'
);

-- Debt/Credit management
UPDATE consumer_complaints
SET product = 'Debt/Credit management'
WHERE product = 'Debt or credit management';

-- Vehicle loan
UPDATE consumer_complaints
SET product = 'Vehicle loan'
WHERE product = 'Vehicle loan or lease';

-- Consumer loan
UPDATE consumer_complaints
SET product = 'Consumer loan'
WHERE product = 'Consumer Loan';