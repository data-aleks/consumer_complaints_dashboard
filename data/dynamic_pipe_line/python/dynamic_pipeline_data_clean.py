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
    ("sql/data_cleaning/cfpb_consumer_complaints_consumer_narrative_cleanup.sql", "Clean Consumer Narrative"),
    ("sql/data_cleaning/cfpb_consumer_complaints_dates_cleanup.sql", "Clean Dates"),
    ("sql/data_cleaning/cfpb_consumer_complaints_product_standartize.sql", "Standardize Products"),
    ("sql/data_cleaning/cfpb_consumer_complaints_state_code_cleanup.sql", "Clean State Codes"),
    ("sql/data_cleaning/cfpb_consumer_complaints_sub_issue_cleanup.sql", "Clean Sub-Issues"),
    ("sql/data_cleaning/cfpb_consumer_complaints_sub_product_cleanup.sql", "Clean Sub-Products"),
    ("sql/data_cleaning/cfpb_consumer_complaints_tags_cleanup.sql", "Clean Tags"),
    ("sql/data_cleaning/cfpb_consumer_complaints_company_public_response_cleanup.sql", "Clean Public Response"),
]

# === MAIN RUN FUNCTION ===
def run(engine=None, limit=None):
    logging.info("Connecting to MySQL...")
    if engine is None:
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
    
    with engine.begin() as conn:
        # 1. Find how many records need cleaning
        count_sql = "SELECT COUNT(*) FROM consumer_complaints_raw WHERE cleaned_timestamp IS NULL"
        if limit:
            count_sql += f" LIMIT {limit}"
        
        count_result = conn.exec_driver_sql(count_sql).scalar()
        logging.info(f"Found {count_result} records ready for cleaning.")
        log_db(engine, "Cleaning Pre-check", "INFO", f"Found {count_result} records to clean.")
        
        if count_result == 0:
            logging.info("No new records to clean. Skipping.")
            return

        # 2. Run all cleaning scripts, which will now operate only on un-cleaned records.
        logging.info("Starting data cleaning pipeline...")
        incremental_clause = "AND cleaned_timestamp IS NULL"
        for script_path, label in cleaning_scripts:
            # Pass the incremental clause to the utility function
            if not execute_sql_file(engine, script_path, label, incremental_clause=incremental_clause, limit=limit):
                raise Exception(f"Failed during cleaning step: {label}")

        # 3. Update the timestamp for the records we just processed.
        logging.info("Updating timestamp for cleaned records...")
        update_sql = "UPDATE consumer_complaints_raw SET cleaned_timestamp = NOW() WHERE cleaned_timestamp IS NULL"
        if limit:
            # This is important to only stamp the records we actually processed if a limit was applied
            update_sql += f" LIMIT {limit}"
        
        conn.exec_driver_sql(update_sql)
        log_db(engine, "Cleaning", "SUCCESS", f"Successfully cleaned and stamped {count_result} records.")

    logging.info("Data cleaning step completed.")

# === CLI ENTRYPOINT ===
if __name__ == "__main__":
    run()