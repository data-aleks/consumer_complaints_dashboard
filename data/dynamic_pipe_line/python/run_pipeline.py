import logging
import sys
import time
import argparse
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import os

# === Import pipeline modules ===
import dynamic_pipeline_data_ingestion as ingestion
import dynamic_pipeline_data_clean as cleaning
import dynamic_pipeline_data_insert as data_insert  # ✅ NEW
import dynamic_pipeline_data_modeling as modeling

# === Load DB config ===
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(connection_string)

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# === Duration tracker ===
step_durations = {}

# === CLI argument parser ===
def parse_args():
    parser = argparse.ArgumentParser(description="Run CFPB ETL pipeline")
    parser.add_argument(
        "--step",
        choices=["all", "ingest", "clean", "insert", "model"],  # ✅ UPDATED
        default="all",
        help="Which pipeline step to run"
    )
    return parser.parse_args()

# === Timed execution wrapper ===
def timed_step(label, func):
    start = time.time()
    logging.info(f"▶️ {label} started")
    func()
    duration = round(time.time() - start, 2)
    step_durations[label] = duration
    logging.info(f"⏱️ {label} completed in {duration} seconds")

# === Run setup scripts if tables are missing ===
def ensure_tables_and_indexes():
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    setup_scripts = {
        "consumer_complaints_raw": "python\\sql\\setup\\create_raw_data_table.sql",
        "consumer_complaints_cleaned": "python\\sql\\setup\\create_cleaned_data_table.sql",
        "ingestion_metadata": "python\\sql\\setup\\create_ingestion_metadata_table.sql"
    }

    for table, script_path in setup_scripts.items():
        if table not in existing_tables:
            logging.info(f"🛠️ Creating missing table: {table}")
            try:
                with open(script_path, "r", encoding="utf-8") as file:
                    sql = file.read()
                with engine.connect() as conn:
                    for stmt in sql.split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            conn.exec_driver_sql(stmt)
                logging.info(f"✅ Created table: {table}")
            except Exception as e:
                logging.error(f"❌ Failed to create table {table}: {e}", exc_info=True)
                sys.exit(1)
        else:
            logging.info(f"✅ Table exists: {table}")

    # === Programmatic index creation ===
    try:
        with engine.connect() as conn:
            indexes_raw = [idx['name'] for idx in inspector.get_indexes('consumer_complaints_raw')]
            indexes_cleaned = [idx['name'] for idx in inspector.get_indexes('consumer_complaints_cleaned')]

            if 'idx_raw_complaint_id' not in indexes_raw:
                conn.exec_driver_sql(
                    "CREATE INDEX idx_raw_complaint_id ON consumer_complaints_raw(`Complaint ID`);"
                )
                logging.info("✅ Created index: idx_raw_complaint_id")
            else:
                logging.info("ℹ️ Index already exists: idx_raw_complaint_id")

            if 'idx_cleaned_complaint_id' not in indexes_cleaned:
                conn.exec_driver_sql(
                    "CREATE INDEX idx_cleaned_complaint_id ON consumer_complaints_cleaned(complaint_id);"
                )
                logging.info("✅ Created index: idx_cleaned_complaint_id")
            else:
                logging.info("ℹ️ Index already exists: idx_cleaned_complaint_id")

    except Exception as e:
        logging.error(f"❌ Failed to create indexes: {e}", exc_info=True)
        sys.exit(1)

# === Multi-statement SQL executor ===
def execute_sql_file_multistatement(script_path, label):
    try:
        with open(script_path, "r", encoding="utf-8") as file:
            sql = file.read()
        with engine.begin() as conn:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.exec_driver_sql(stmt)
        logging.info(f"✅ Executed: {label}")
    except Exception as e:
        logging.error(f"❌ Error in {label}: {e}", exc_info=True)
        sys.exit(1)

# === Main pipeline runner ===
def run_pipeline(step):
    try:
        if step in ["all", "ingest"]:
            ensure_tables_and_indexes()
            timed_step("Data Ingestion", lambda: ingestion.run(engine))
        
        if step in ["all", "insert"]: 
            timed_step("Data Insert", lambda: data_insert.run(engine))

        if step in ["all", "clean"]:
            timed_step("Data Cleaning", lambda: cleaning.run(engine, execute_sql_file_multistatement))

        if step in ["all", "model"]:
            timed_step("Data Modeling", lambda: modeling.run(engine, execute_sql_file_multistatement))

        logging.info("✅ Pipeline completed successfully.")

        # Final summary
        print("\n📊 Pipeline Summary:")
        for label, duration in step_durations.items():
            print(f"- {label}: {duration} seconds")

    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

# === Entry point ===
if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.step)