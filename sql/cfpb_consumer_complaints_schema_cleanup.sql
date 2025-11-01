-- CFPB Consumer Complaints Schema Cleanup
-- This script renames columns for clarity and consistency
-- Intended for use with MySQL 8.0+

ALTER TABLE consumer_complaints
  CHANGE COLUMN `Company` company_name TEXT,
  CHANGE COLUMN `Company public response` company_public_response TEXT,
  CHANGE COLUMN `Company response to consumer` company_response TEXT,
  CHANGE COLUMN `Complaint ID` complaint_id BIGINT,
  CHANGE COLUMN `Consumer complaint narrative` consumer_narrative TEXT,
  CHANGE COLUMN `Consumer consent provided?` consumer_consent TEXT,
  CHANGE COLUMN `Consumer disputed?` consumer_disputed TEXT,
  CHANGE COLUMN `Date received` date_received TEXT,
  CHANGE COLUMN `Date sent to company` date_sent_to_company TEXT,
  CHANGE COLUMN `Issue` issue TEXT,
  CHANGE COLUMN `Product` product TEXT,
  CHANGE COLUMN `State` state_code TEXT,
  CHANGE COLUMN `Sub-issue` sub_issue TEXT,
  CHANGE COLUMN `Sub-product` sub_product TEXT,
  CHANGE COLUMN `Submitted via` submitted_via TEXT,
  CHANGE COLUMN `Tags` tags TEXT,
  CHANGE COLUMN `Timely response?` timely_response TEXT,
  CHANGE COLUMN `ZIP code` zip_code TEXT;