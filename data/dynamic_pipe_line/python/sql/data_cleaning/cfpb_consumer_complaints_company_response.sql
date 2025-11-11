-- Standardizes 'Company response to consumer' by setting empty strings to NULL.
-- Standardizes 'Company response to consumer' by shortening the "Closed with..." values.
UPDATE consumer_complaints_raw
SET company_response_to_consumer = CASE
    WHEN company_response_to_consumer = 'Closed with explanation' THEN 'Explanation'
    WHEN company_response_to_consumer = 'Closed with non-monetary relief' THEN 'Non-monetary relief'
    WHEN company_response_to_consumer = 'Closed with monetary relief' THEN 'Monetary relief'
    WHEN company_response_to_consumer = 'Closed with relief' THEN 'Relief'
    WHEN company_response_to_consumer = 'Closed without relief' THEN 'No relief'
    ELSE company_response_to_consumer -- Keep other values like 'Closed', 'In progress', etc., as they are.
END
WHERE company_response_to_consumer IN (
    'Closed with explanation',
    'Closed with non-monetary relief',
    'Closed with monetary relief',
    'Closed with relief',
    'Closed without relief'
) {incremental_clause} {limit_clause};