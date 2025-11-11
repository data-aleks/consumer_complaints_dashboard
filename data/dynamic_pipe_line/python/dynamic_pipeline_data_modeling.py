import logging
from dotenv import load_dotenv
from sqlalchemy import text
from pipeline_logger import log_db
from pipeline_utils import execute_sql_file
import os

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))

# SQL script paths for data modeling
modeling_scripts = [
    ("sql/data_modeling/cfpb_consumer_complaints_dim_company.sql", "Create company dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_consent.sql", "Create consent dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_date.sql", "Create date dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_disputed.sql", "Create disputed dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_issue.sql", "Create issue dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_origin.sql", "Create origin dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_product.sql", "Create product dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_public_response.sql", "Create public_response dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_company_response.sql", "Create company_response dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_sub_issue.sql", "Create sub_issue dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_sub_product.sql", "Create sub_product dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_tag.sql", "Create tag dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_dim_zip_code.sql", "Create zip_code dimension"),
    ("sql/data_modeling/cfpb_consumer_complaints_fact_complaint.sql", "Create fact_complaint table"),
]

# === MAIN RUN FUNCTION ===
def run(engine, limit=None):
    try:
        with engine.begin() as conn:
            # 1. Find how many records need modeling
            count_sql = "SELECT COUNT(*) FROM consumer_complaints_cleaned WHERE modeling_timestamp IS NULL"
            count_result = conn.exec_driver_sql(count_sql).scalar()
            logging.info(f"Found {count_result} records ready for modeling.")
            log_db(engine, "Modeling Pre-check", "INFO", f"Found {count_result} records to model.")

            if count_result == 0:
                logging.info("No new records to model. Skipping.")
                return

            # 2. Run modeling scripts
            logging.info("Starting data modeling pipeline...")
            for script_path, label in modeling_scripts:
                # Dimension tables are rebuilt each time, fact table is incremental
                incremental_clause = "AND c.modeling_timestamp IS NULL" if "fact_complaint" in script_path else None
                
                if not execute_sql_file(engine, script_path, label, incremental_clause=incremental_clause, limit=limit):
                     raise Exception(f"Failed during modeling step: {label}")


            # 3. Update the timestamp for the records we just modeled.
            logging.info("Updating timestamp for modeled records...")
            update_sql = "UPDATE consumer_complaints_cleaned SET modeling_timestamp = NOW() WHERE modeling_timestamp IS NULL"
            if limit:
                update_sql += f" LIMIT {limit}"
            conn.execute(text(update_sql))
            log_db(engine, "Modeling", "SUCCESS", f"Successfully modeled and stamped {count_result} records.")

        logging.info("All data modeling steps completed.")
    except Exception as e:
        logging.error(f"Data modeling pipeline failed: {e}", exc_info=True)
        log_db(engine, "Data Modeling", "ERROR", f"Pipeline failed: {e}")
        raise