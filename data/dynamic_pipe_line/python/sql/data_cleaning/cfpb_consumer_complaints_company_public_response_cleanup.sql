-- Standardizes 'Company public response' by mapping known values and setting empty/nulls/None to 'N/A'
UPDATE consumer_complaints_staging
SET company_public_response = CASE
    -- 1. Handle Null, Empty, and existing 'None' values, mapping to 'N/A'
    WHEN company_public_response IS NULL 
    OR TRIM(company_public_response) = '' 
    OR company_public_response = 'None' 
    THEN 'N/A'

    -- 2. Standardized response categories (Shortened for clarity)
    WHEN company_public_response = 'Company believes complaint caused principally by actions of third party outside the control or direction of the company' 
    THEN 'Third Party Responsibility'
    
    WHEN company_public_response = 'Company believes complaint is the result of an isolated error' 
    THEN 'Isolated Error'
    
    WHEN company_public_response = 'Company believes complaint relates to a discontinued policy or procedure' 
    THEN 'Discontinued Policy/Procedure'
    
    WHEN company_public_response = 'Company believes complaint represents an opportunity for improvement to better serve consumers' 
    THEN 'Opportunity for Improvement'
    
    WHEN company_public_response = 'Company believes it acted appropriately as authorized by contract or law' 
    THEN 'Acted per Contract/Law'
    
    WHEN company_public_response = 'Company believes the complaint is the result of a misunderstanding' 
    THEN 'Result of Misunderstanding'
    
    WHEN company_public_response = 'Company believes the complaint provided an opportunity to answer consumer''s questions' 
    THEN 'Consumer Question Answered'
    
    WHEN company_public_response = 'Company can''t verify or dispute the facts in the complaint' 
    THEN 'Cannot Verify/Dispute Facts'
    
    WHEN company_public_response IN (
        'Company chooses not to provide a public response',
        'Company has responded to the consumer and the CFPB and chooses not to provide a public response'
    ) THEN 'No Public Response'
    
    WHEN company_public_response = 'Company disputes the facts presented in the complaint' 
    THEN 'Disputes Complaint Facts'

    -- 3. Preserve original if no match (shouldn't happen with the above list but remains for safety)
    ELSE company_public_response 
END
WHERE 1=1
{limit_clause};