import logging
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pipeline_logger import log_db
from pipeline_utils import execute_sql_file
import os

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# SQL script paths for data cleaning
cleaning_scripts = [
    ("sql/data_cleaning/cfpb_consumer_complaints_company_response.sql", "Clean Company Response"),
    ("sql/data_cleaning/cfpb_consumer_complaints_consumer_consent_cleanup.sql", "Clean Consumer Consent"),
    ("sql/data_cleaning/cfpb_consumer_complaints_consumer_disputed_cleanup.sql", "Clean Consumer Disputed"),
    ("sql/data_cleaning/cfpb_consumer_complaints_consumer_narrative_cleanup.sql", "Clean Consumer Narrative"),
    ("sql/data_cleaning/cfpb_consumer_complaints_dates_cleanup.sql", "Clean Dates"),
    ("sql/data_cleaning/cfpb_consumer_complaints_product_standartize.sql", "Standardize Products"),
    ("sql/data_cleaning/cfpb_consumer_complaints_state_code_cleanup.sql", "Clean State Codes"),
    ("sql/data_cleaning/cfpb_consumer_complaints_issue_cleanup.sql", "Clean Issues"),
    ("sql/data_cleaning/cfpb_consumer_complaints_sub_issue_cleanup.sql", "Clean Sub-Issues"),
    ("sql/data_cleaning/cfpb_consumer_complaints_sub_product_cleanup.sql", "Clean Sub-Products"),
    ("sql/data_cleaning/cfpb_consumer_complaints_tags_cleanup.sql", "Clean Tags"),
    ("sql/data_cleaning/cfpb_consumer_complaints_company_public_response_cleanup.sql", "Clean Public Response"),
    ("sql/data_cleaning/cfpb_consumer_complaints_timely_response_cleanup.sql", "Clean Timely Response"),
]

# === MAIN RUN FUNCTION ===
def run(engine=None, limit=None):
    logging.info("Connecting to MySQL...")
    if engine is None:
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
    
    with engine.begin() as conn:
        # 1. Find how many records are in the staging table to be cleaned.
        count_sql = "SELECT COUNT(*) FROM consumer_complaints_staging"
        if limit:
            count_sql += f" LIMIT {limit}"
        
        count_result = conn.exec_driver_sql(count_sql).scalar()
        logging.info(f"Found {count_result} records in staging table ready for cleaning.")
        log_db(engine, "Cleaning Pre-check", "INFO", f"Found {count_result} records to clean.")
        
        if count_result == 0:
            logging.info("Staging table is empty. No records to clean. Skipping.")
            return

        # 2. Run all cleaning scripts against the staging table.
        logging.info("Starting data cleaning pipeline...")
        for script_path, label in cleaning_scripts:
            # The incremental_clause is no longer needed as we operate on a fresh staging table.
            if not execute_sql_file(engine, script_path, label, limit=limit):
                raise Exception(f"Failed during cleaning step: {label}")

        # 3. Update the timestamp in the staging table to mark records as cleaned.
        logging.info("Updating timestamp for cleaned records in staging table...")
        update_sql = "UPDATE consumer_complaints_staging SET cleaned_timestamp = NOW()"
        conn.exec_driver_sql(update_sql)

        # 4. Update the timestamp in the original raw table for data lineage.
        logging.info("Updating timestamp in raw table for processed records...")
        update_raw_sql = """
            UPDATE consumer_complaints_raw r
            JOIN consumer_complaints_staging s ON r.complaint_id = s.complaint_id
            SET r.cleaned_timestamp = s.cleaned_timestamp
            WHERE r.cleaned_timestamp IS NULL;
        """
        conn.exec_driver_sql(update_raw_sql)
        log_db(engine, "Cleaning", "SUCCESS", f"Successfully cleaned and stamped {count_result} records in staging and raw tables.")

    logging.info("Data cleaning step completed.")

# === CLI ENTRYPOINT ===
if __name__ == "__main__":
    run()