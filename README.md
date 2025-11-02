# Consumer Complaints Dashboard
A business intelligence case study using Power BI and data analytics

## Table of Contents
- [Project Introduction](#project-introduction)
- [Dataset Information](#dataset-information)
- [Key Insights From Data](#key-insights-from-data)
- [Business Recommendation](#business-recommendation)

## Project Introduction
There is a need for a tool that helps our company identify and address emerging issues before they become public complaints. The goal is to make smarter, data-driven decisions about where to improve service, how to communicate with customers, and how to stay ahead of reputational and regulatory risks.

By building a Power BI dashboard using real-world data from the CFPB Consumer Complaint Database, the analytics team can uncover patterns in complaint volume, product categories, submission channels, geographic distribution, and company responsiveness—helping stakeholders visualize industry-wide friction points and anticipate potential vulnerabilities.

This project aims to strengthen customer satisfaction, enhance operational agility, and ensure our company remains proactive in addressing consumer concerns.

**This project uses real data from the Consumer Complaint Database maintained by the Consumer Financial Protection Bureau.**

## Dataset Information 
This is real world data sourced via [Consumer Financial Protection Bureau](https://www.consumerfinance.gov/data-research/consumer-complaints/#get-the-data). The data is downloaded as bulk CSV file, added to a local MySQL server to then be imported in to Power BI. 

### Dataset Structure
| Feature Name                 | Description                                |
|------------------------------|--------------------------------------------|
| date_received                | date complain received                     |
| product                      | product category                           |
| sub_product                  |  sub-product type                          |
| issue                        | Description of the consumer’s issue        |
| sub_issue                    | More specific issue detail (if available)  |
| consumer_narrative           | Consent status                             |
| company_public_response      | Loan amount                                |
| company_name                 | Interest rate                              |
| state_code                   | U.S. state or territory code               |
| zip_code                     | ZIP code                                   |
| tags                         | Special population flags                   |
| submitted_via                | Submission channel                         |
| date_sent_to_company         | Credit history length                      |
| company_response             | Company’s response status                  |
| timely_response              | Whether the company responded in time      |
| consumer_disputed            | Whether the consumer disputed the response |
| complaint_id                 | Unique identifier for each complaint       |

### Data Model
Data model is created from complaints data inside MySQL instead of Power BI

| Table Name                 | Description                                |
|----------------------------|--------------------------------------------|
| dim_date                   | date dimension table                       |
| dim_proudct                | product category dimension table           |
| dim_sub_product            |  sub-product type dimension table          |
| dim_issue                  | consumer’s issue dimension table           |
| dim_sub_issue              | sub issue dimension table                  |
| dim_consent                | Consent dimension table                    |
| dim_public_response        | public resposne dimension table            |
| dim_company                | company dimension table                    |
| dim_state                  | state dimension table                      |
| dim_zip_code               | zip code dimension table                   |
| dim_tag                    | population flags dimension table           |
| dim_origin                 | origin dimension table prev submitted_via  |
| dim_status                 | status dimension table prev company_status |
| dim_disputed               | disputed dimension table                   |
| fact_complaints            | complaints fact table                      |

This is how it looks like when imported in to Power BI via MySQL connector:
![Dashboard Data Model](./screenshots/star_schema_data_model.png "Dashboard star schema data model")

## Key Insights From Data

## Business Recommendation
