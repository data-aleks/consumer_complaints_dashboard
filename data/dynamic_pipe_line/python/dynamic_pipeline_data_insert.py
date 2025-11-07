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

# === SCRIPT CONFIG ===
insert_script_path = "python\sql\data_insertion\cfpb_consumer_complaints_data_insert.sql"
insert_label = "Insert Cleaned Data"

# === LOGGING SETUP ===
log_file = "cfpb_data_insert_log.txt"
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

# === EXECUTION FUNCTION ===
def run_insert_script(engine, script_path, label):
    try:
        logging.info(f"🚀 Starting: {label}")
        with open(script_path, "r", encoding="utf-8") as file:
            sql = file.read()

        start_time = time.time()
        with engine.begin() as conn:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.exec_driver_sql(stmt)

        duration = round(time.time() - start_time, 2)
        logging.info(f"✅ Completed: {label} in {duration}s")
        log_action(label, "success", f"Executed successfully in {duration}s")

    except Exception as e:
        logging.error(f"❌ Error in {label}: {str(e)}", exc_info=True)
        log_action(label, "error", str(e))

# === MAIN RUN FUNCTION ===
def run(engine=None):
    logging.info("🔗 Connecting to MySQL...")
    if engine is None:
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

    logging.info("📥 Starting data insertion step...")
    run_insert_script(engine, insert_script_path, insert_label)
    logging.info("🏁 Data insertion step completed.")

# === CLI ENTRYPOINT ===
if __name__ == "__main__":
    run()