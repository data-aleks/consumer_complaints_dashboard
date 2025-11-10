import logging
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pipeline_logger import log_db
import os

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# === SCRIPT CONFIG ===
insert_script_path = "sql/data_insertion/cfpb_consumer_complaints_data_insert.sql"
insert_label = "Insert Cleaned Data"

# === EXECUTION FUNCTION ===
def run_insert_script(engine, script_path, label, limit=None):
    try:
        with open(script_path, "r", encoding="utf-8") as file:
            sql = file.read()

        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        sql = sql.replace("{limit_clause}", limit_clause)

        with engine.begin() as conn:
            for stmt in sql.split(';'):
                stmt = stmt.strip()
                if stmt:
                    conn.exec_driver_sql(stmt)
        logging.info(f"Executed: {label}")
    except Exception as e:
        logging.error(f"Error in {label}: {str(e)}", exc_info=True)
        log_db(engine, label, "ERROR", str(e))
        raise

# === MAIN RUN FUNCTION ===
def run(engine=None, limit=None):
    logging.info("Connecting to MySQL...")
    if engine is None:
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
    
    with engine.begin() as conn:
        # 1. Find how many records are cleaned but not yet inserted.
        count_sql = """
            SELECT COUNT(r.complaint_id)
            FROM consumer_complaints_raw r
            LEFT JOIN consumer_complaints_cleaned c ON r.complaint_id = c.complaint_id
            WHERE r.cleaned_timestamp IS NOT NULL AND c.complaint_id IS NULL
        """
        count_result = conn.exec_driver_sql(count_sql).scalar()
        logging.info(f"Found {count_result} records ready for insertion.")
        log_db(engine, "Insertion Pre-check", "INFO", f"Found {count_result} records to insert.")

        if count_result == 0:
            logging.info("No new records to insert. Skipping.")
            return

        # 2. Run the insertion script.
        logging.info("Starting data insertion step...")
        run_insert_script(engine, insert_script_path, insert_label, limit=limit)
        log_db(engine, "Insertion", "SUCCESS", f"Successfully inserted {count_result} records.")

    logging.info("Data insertion step completed.")

# === CLI ENTRYPOINT ===
if __name__ == "__main__":
    run()