-- Nullifies invalid state codes.
UPDATE consumer_complaints_raw
SET state = NULL
WHERE state IN ('AE', 'AP', 'FM', 'GU', 'MH', 'MP', 'PW', 'PR', 'VI', 'AS') {incremental_clause} {limit_clause};