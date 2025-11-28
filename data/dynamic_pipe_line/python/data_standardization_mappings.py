"""
This module centralizes all dictionaries used for data standardization in the pipeline.
Separating these mappings from the cleaning logic improves maintainability and readability.
"""

# Mapping for standardizing state names to abbreviations.
STATE_MAP = {
    'UNITED STATES MINOR OUTLYING ISLANDS': 'UM', 'PUERTO RICO': 'PR', 'VIRGIN ISLANDS': 'VI',
    'GUAM': 'GU', 'AMERICAN SAMOA': 'AS', 'NORTHERN MARIANA ISLANDS': 'MP', 'DISTRICT OF COLUMBIA': 'DC',
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
    'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH',
    'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
    'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    # Armed Forces codes
    'ARMED FORCES AMERICAS': 'AA',
    'ARMED FORCES EUROPE': 'AE',
    'ARMED FORCES PACIFIC': 'AP'
}

# Mapping for standardizing company public response text.
PUB_RESPONSE_MAP = {
    'COMPANY BELIEVES COMPLAINT CAUSED PRINCIPALLY BY ACTIONS OF THIRD PARTY OUTSIDE THE CONTROL OR DIRECTION OF THE COMPANY': 'Third Party Responsibility',
    'COMPANY BELIEVES COMPLAINT IS THE RESULT OF AN ISOLATED ERROR': 'Isolated Error',
    'COMPANY BELIEVES COMPLAINT RELATES TO A DISCONTINUED POLICY OR PROCEDURE': 'Discontinued Policy/Procedure',
    'COMPANY BELIEVES COMPLAINT REPRESENTS AN OPPORTUNITY FOR IMPROVEMENT TO BETTER SERVE CONSUMERS': 'Opportunity for Improvement',
    'COMPANY BELIEVES IT ACTED APPROPRIATELY AS AUTHORIZED BY CONTRACT OR LAW': 'Acted per Contract/Law',
    'COMPANY BELIEVES THE COMPLAINT IS THE RESULT OF A MISUNDERSTANDING': 'Result of Misunderstanding',
    'COMPANY BELIEVES THE COMPLAINT PROVIDED AN OPPORTUNITY TO ANSWER CONSUMER\'S QUESTIONS': 'Consumer Question Answered',
    'COMPANY CAN\'T VERIFY OR DISPUTE THE FACTS IN THE COMPLAINT': 'Cannot Verify/Dispute Facts',
    'COMPANY CHOOSES NOT TO PROVIDE A PUBLIC RESPONSE': 'No Public Response',
    'COMPANY HAS RESPONDED TO THE CONSUMER AND THE CFPB AND CHOOSES NOT TO PROVIDE A PUBLIC RESPONSE': 'No Public Response',
    'COMPANY DISPUTES THE FACTS PRESENTED IN THE COMPLAINT': 'Disputes Complaint Facts'
}

# Mapping for standardizing company response to consumer text.
COMP_RESPONSE_MAP = {
    'CLOSED WITH MONETARY RELIEF': 'Monetary Relief',
    'CLOSED WITH NON-MONETARY RELIEF': 'Non-monetary Relief',
    'CLOSED WITH RELIEF': 'Unspecified Relief',
    'CLOSED WITHOUT RELIEF': 'No Relief',
    'CLOSED WITH EXPLANATION': 'Explanation Provided'
}

# Mapping for standardizing tags.
TAGS_MAP = {'OLDER AMERICAN': 'Older American', 'SERVICEMEMBER': 'Servicemember', 'OLDER AMERICAN, SERVICEMEMBER': 'Older American & Servicemember'}

# Mapping for standardizing consumer consent status.
CONSENT_MAP = {'CONSENT PROVIDED': 'Provided', 'CONSENT NOT PROVIDED': 'Not provided', 'CONSENT WITHDRAWN': 'Withdrawn'}

# Mapping for standardizing consumer disputed status.
DISPUTED_MAP = {'YES': 'Yes', 'NO': 'No', '1': 'Yes', '0': 'No'}

# Mapping for standardizing product names.
PRODUCT_MAP = {
    'CHECKING OR SAVINGS ACCOUNT': 'Deposit Account (Checking/Savings)',
    'BANK ACCOUNT OR SERVICE': 'General Bank Service',
    'CREDIT CARD': 'Credit Card', 'CREDIT CARD OR PREPAID CARD': 'Credit Card',
    'PREPAID CARD': 'Prepaid Card',
    'CREDIT REPORTING': 'Credit Reporting/Repair Service',
    'CREDIT REPORTING OR OTHER PERSONAL CONSUMER REPORTS': 'Credit Reporting/Repair Service',
    'CREDIT REPORTING, CREDIT REPAIR SERVICES, OR OTHER PERSONAL CONSUMER REPORTS': 'Credit Reporting/Repair Service',
    'DEBT COLLECTION': 'Debt Collection/Management', 'DEBT OR CREDIT MANAGEMENT': 'Debt Collection/Management',
    'MONEY TRANSFER, VIRTUAL CURRENCY, OR MONEY SERVICE': 'Money Transfer Service', 'MONEY TRANSFERS': 'Money Transfer Service',
    'VIRTUAL CURRENCY': 'Virtual Currency',
    'MORTGAGE': 'Mortgage',
    'PAYDAY LOAN': 'Payday/Title/Advance Loan', 'PAYDAY LOAN, TITLE LOAN, OR PERSONAL LOAN': 'Payday/Title/Advance Loan',
    'PAYDAY LOAN, TITLE LOAN, PERSONAL LOAN, OR ADVANCE LOAN': 'Payday/Title/Advance Loan',
    'STUDENT LOAN': 'Student Loan',
    'VEHICLE LOAN OR LEASE': 'Vehicle Loan/Lease',
    'CONSUMER LOAN': 'Personal Loan',
    'OTHER FINANCIAL SERVICE': 'Other Financial Service'
}

# Mapping for standardizing issue descriptions.
ISSUE_MAP = {
    'INCORRECT INFORMATION ON YOUR REPORT': 'Credit Reporting & Investigation', 'INCORRECT INFORMATION ON CREDIT REPORT': 'Credit Reporting & Investigation',
    'PROBLEM WITH A COMPANY\'S INVESTIGATION INTO AN EXISTING PROBLEM': 'Credit Reporting & Investigation',
    'PROBLEM WITH A CREDIT REPORTING COMPANY\'S INVESTIGATION INTO AN EXISTING PROBLEM': 'Credit Reporting & Investigation',
    'CREDIT REPORTING COMPANY\'S INVESTIGATION': 'Credit Reporting & Investigation',
    'PROBLEM WITH A COMPANY\'S INVESTIGATION INTO AN EXISTING ISSUE': 'Credit Reporting & Investigation',
    'IMPROPER USE OF YOUR REPORT': 'Credit Reporting & Investigation', 'IMPROPER USE OF MY CREDIT REPORT': 'Credit Reporting & Investigation',
    'PROBLEM WITH A PURCHASE SHOWN ON YOUR STATEMENT': 'Credit Reporting & Investigation',
    'CREDIT REPORTING, FRAUD, OR IDENTITY THEFT': 'Fraud & Identity Theft',
    'ATTEMPTS TO COLLECT DEBT NOT OWED': 'Debt Validity & Disputed Debt', 'CONT\'D ATTEMPTS COLLECT DEBT NOT OWED': 'Debt Validity & Disputed Debt',
    'FALSE STATEMENTS OR REPRESENTATION': 'Debt Validity & Disputed Debt', 'WRITTEN NOTIFICATION ABOUT DEBT': 'Debt Validity & Disputed Debt',
    'DISCLOSURE VERIFICATION OF DEBT': 'Debt Validity & Disputed Debt',
    'COMMUNICATION TACTICS': 'Collection Communication & Tactics', 'ELECTRONIC COMMUNICATIONS': 'Collection Communication & Tactics',
    'THREATENED TO CONTACT SOMEONE OR SHARE INFORMATION IMPROPERLY': 'Collection Communication & Tactics',
    'TAKING/THREATENING AN ILLEGAL ACTION': 'Collection Communication & Tactics',
    'STRUGGLING TO PAY MORTGAGE': 'Loan Hardship & Repayment', 'STRUGGLING TO REPAY YOUR LOAN': 'Loan Hardship & Repayment',
    'CAN\'T REPAY MY LOAN': 'Loan Hardship & Repayment', 'STRUGGLING TO PAY YOUR LOAN': 'Loan Hardship & Repayment',
    'STRUGGLING TO PAY YOUR BILL': 'Loan Hardship & Repayment',
    'MANAGING AN ACCOUNT': 'Account/Card Management & Access', 'OPENING AN ACCOUNT': 'Account/Card Management & Access',
    'CLOSING YOUR ACCOUNT': 'Account/Card Management & Access', 'CLOSING AN ACCOUNT': 'Account/Card Management & Access',
    'PROBLEM GETTING A CARD OR CLOSING AN ACCOUNT': 'Account/Card Management & Access',
    'PROBLEM ACCESSING ACCOUNT': 'Account/Card Management & Access', 'PROBLEM WITH A PURCHASE OR TRANSFER': 'Account/Card Management & Access'
}

# Mapping for standardizing sub-product names.
SUB_PRODUCT_MAP = {
    'CREDIT CARD': 'Credit Card', 'STORE CREDIT CARD': 'Credit Card', 'GENERAL-PURPOSE CREDIT CARD OR CHARGE CARD': 'Credit Card',
    'PAYDAY LOAN': 'Payday Loan', 'TITLE LOAN': 'Title Loan', 'PAWN LOAN': 'Pawn Loan',
    'VEHICLE LOAN': 'Vehicle Loan/Lease', 'AUTO': 'Vehicle Loan/Lease', 'VEHICLE LEASE': 'Vehicle Loan/Lease',
    'CONVENTIONAL HOME MORTGAGE': 'General/Conventional Mortgage', 'CONVENTIONAL FIXED MORTGAGE': 'General/Conventional Mortgage',
    'FHA MORTGAGE': 'FHA Mortgage', 'VA MORTGAGE': 'VA Mortgage',
    'FEDERAL STUDENT LOAN': 'Federal Student Loan', 'PRIVATE STUDENT LOAN': 'Private/Non-Federal Student Loan',
    'DOMESTIC (US) MONEY TRANSFER': 'Money Transfer', 'INTERNATIONAL MONEY TRANSFER': 'Money Transfer',
    'VIRTUAL CURRENCY': 'Virtual Currency',
    'CHECKING ACCOUNT': 'Checking Account', 'SAVINGS ACCOUNT': 'Savings Account',
    'CD (CERTIFICATE OF DEPOSIT)': 'Certificate of Deposit'
}

# Mapping for standardizing sub-issue descriptions.
SUB_ISSUE_MAP = {
    'UNEXPECTED FEES': 'Fees & Charges', 'LATE FEE': 'Fees & Charges',
    'LOAN SERVICING, PAYMENTS, ESCROW ACCOUNT': 'Loan Servicing & Modification',
    'INCORRECT INFORMATION ON YOUR REPORT': 'Credit Reporting & Data Accuracy',
    'FRAUD OR SCAM': 'Fraud & Unauthorized Activity',
    'PROBLEM WITH CUSTOMER SERVICE': 'Customer Service & Support',
    'ACCOUNT OPENING, CLOSING, OR MANAGEMENT': 'Account Management',
    'PAYMENT TO ACCT NOT CREDITED': 'Billing & Payment Issues',
    'COLLECTION PRACTICES': 'Debt Collection Practices'
}