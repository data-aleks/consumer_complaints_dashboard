import logging
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
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
    ("python/sql/data_cleaning/cfpb_consumer_complaints_company_response.sql", "Clean Company Response"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_consumer_consent_cleanup.sql", "Clean Consumer Consent"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_consumer_narrative_cleanup.sql", "Clean Consumer Narrative"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_dates_cleanup.sql", "Clean Dates"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_product_standartize.sql", "Standardize Products"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_state_code_cleanup.sql", "Clean State Codes"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_sub_issue_cleanup.sql", "Clean Sub-Issues"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_sub_product_cleanup.sql", "Clean Sub-Products"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_tags_cleanup.sql", "Clean Tags"),
    ("python/sql/data_cleaning/cfpb_consumer_complaints_company_public_response_cleanup.sql", "Clean Public Response"),
]

# === LOGGING SETUP ===
log_file = "cfpb_data_cleaning_log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def log_action(action, status, message):
    if status.lower() == "success":
        logging.info(f"{action} | SUCCESS | {message}")
    else:
        logging.error(f"{action} | ERROR | {message}")

# === MULTI-STATEMENT EXECUTION FUNCTION ===
def run_multistatement_cleaning_script(engine, script_path, label):
    try:
        logging.info(f"🚀 Starting: {label}")
        with open(script_path, "r", encoding="utf-8") as file:
            sql = file.read()

        with engine.begin() as conn:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.exec_driver_sql(stmt)

        logging.info(f"✅ Completed: {label}")
        log_action(label, "success", "Executed all statements successfully")
    except Exception as e:
        logging.error(f"❌ Error in {label}: {str(e)}", exc_info=True)
        log_action(label, "error", str(e))

# === BATCHED EXECUTION FUNCTION ===
def run_batched_cleaning_script(engine, script_path, label, batch_size=1000):
    try:
        logging.info(f"🚀 Starting: {label}")
        with open(script_path, "r", encoding="utf-8") as file:
            base_sql = file.read().strip()

        with engine.connect() as conn:
            last_id = 0
            batch_num = 1
            while True:
                start_time = time.time()
                batched_sql = base_sql.replace("{last_id}", str(last_id)).replace("{batch_size}", str(batch_size))
                result = conn.exec_driver_sql(batched_sql)
                affected = result.rowcount if result.returns_rows else "unknown"
                duration = round(time.time() - start_time, 2)

                logging.info(f"✅ {label} | Batch {batch_num} | Last ID: {last_id} | Rows: {affected} | Time: {duration}s")
                log_action(f"{label} Batch {batch_num}", "success", f"Processed batch after ID {last_id}, affected rows: {affected}, duration: {duration}s")

                if affected == 0 or affected is None:
                    logging.info(f"🏁 Completed: {label}")
                    break

                last_id_query = f"SELECT MAX(`Complaint ID`) FROM consumer_complaints_raw WHERE `Complaint ID` > {last_id} LIMIT {batch_size}"
                max_id_result = conn.exec_driver_sql(last_id_query).scalar()
                if not max_id_result:
                    logging.info(f"🏁 Completed: {label}")
                    break

                last_id = max_id_result
                batch_num += 1

    except Exception as e:
        logging.error(f"❌ Error in {label}: {str(e)}", exc_info=True)
        log_action(label, "error", str(e))

# === MAIN RUN FUNCTION ===
def run(engine=None, sql_executor=None):
    logging.info("🔗 Connecting to MySQL...")
    if engine is None:
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

    logging.info("🧼 Starting data cleaning pipeline...")
    for script_path, label in cleaning_scripts:
        with open(script_path, "r", encoding="utf-8") as file:
            sql = file.read()
        if "{last_id}" in sql and "{batch_size}" in sql:
            run_batched_cleaning_script(engine, script_path, label)
        else:
            run_multistatement_cleaning_script(engine, script_path, label)

    logging.info("✅ All data cleaning steps completed.")

# === CLI ENTRYPOINT ===
if __name__ == "__main__":
    run()