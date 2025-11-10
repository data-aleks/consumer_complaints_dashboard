import logging
import sys
import time
import argparse
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os

# === Import pipeline modules ===
import dynamic_pipeline_data_ingestion as ingestion
import dynamic_pipeline_data_clean as cleaning
import dynamic_pipeline_data_insert as data_insert  # ✅ NEW
from pipeline_logger import log_db
from pipeline_utils import execute_sql_file
import dynamic_pipeline_data_modeling as modeling

# === Load DB config ===
load_dotenv(dotenv_path=".db_config.env")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before using
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=False
)

# === Logging setup ===
log_file = "pipeline_run.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout) # StreamHandler uses stdout's encoding
    ]
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
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of records to ingest (for development)."
    )
    return parser.parse_args()

# === Timed execution wrapper ===
def timed_step(label, func):
    start = time.time()
    logging.info(f"{label} started")
    func()
    duration = round(time.time() - start, 2)
    step_durations[label] = duration
    logging.info(f"{label} completed in {duration} seconds")

# === Schema migration for metadata table ===
def ensure_metadata_schema():
    inspector = inspect(engine)
    columns = [col['name'] for col in inspector.get_columns('ingestion_metadata')]
    if 'last_modified_date' not in columns:
        logging.info("Adding 'last_modified_date' to 'ingestion_metadata' table...")
        try:
            with engine.connect() as conn:
                conn.exec_driver_sql("ALTER TABLE ingestion_metadata ADD COLUMN last_modified_date DATETIME NULL;")
            logging.info("Column 'last_modified_date' added.")
        except Exception as e:
            logging.error(f"Failed to add 'last_modified_date' column: {e}", exc_info=True)
            sys.exit(1)

def run_migrations():
    """
    A simple migration runner to add new columns if they don't exist.
    """
    inspector = inspect(engine)
    
    # Migration for cleaned_timestamp
    columns_raw = [col['name'] for col in inspector.get_columns('consumer_complaints_raw')]
    if 'cleaned_timestamp' not in columns_raw:
        if not execute_sql_file(engine, os.path.join("sql", "data_insertion", "add_cleaned_timestamp.sql"), "Migration: add_cleaned_timestamp"):
            sys.exit(1)

    # Migration for modeling_timestamp
    columns_cleaned = [col['name'] for col in inspector.get_columns('consumer_complaints_cleaned')]
    if 'modeling_timestamp' not in columns_cleaned:
        if not execute_sql_file(engine, os.path.join("sql", "data_insertion", "add_modeling_timestamp.sql"), "Migration: add_modeling_timestamp"):
            sys.exit(1)


# === Run setup scripts if tables are missing ===
def ensure_tables_and_indexes():
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    setup_scripts = {
        "consumer_complaints_raw": os.path.join("sql", "setup", "create_raw_data_table.sql"),
        "consumer_complaints_cleaned": os.path.join("sql", "setup", "create_cleaned_data_table.sql"),
        "ingestion_metadata": os.path.join("sql", "setup", "create_ingestion_metadata_table.sql"),
        "pipeline_logs": os.path.join("sql", "setup", "create_pipeline_logs_table.sql")
    }

    for table, script_path in setup_scripts.items():
        if table not in existing_tables:
            logging.info(f"Creating missing table: {table}")
            if not execute_sql_file(engine, script_path, f"Create table: {table}"):
                logging.error(f"Failed to create table {table}, exiting.")
                sys.exit(1)
        else:
            logging.info(f"Table exists: {table}")

    # Run migrations now that we are sure the tables exist.
    # This will add columns like 'cleaned_timestamp' if they are missing.
    run_migrations()

    # === Programmatic index creation ===
    try:
        with engine.begin() as conn:
            indexes_raw = [idx['name'] for idx in inspector.get_indexes('consumer_complaints_raw')]
            indexes_cleaned = [idx['name'] for idx in inspector.get_indexes('consumer_complaints_cleaned')]

            if 'idx_raw_complaint_id' not in indexes_raw:
                conn.exec_driver_sql(
                    "CREATE INDEX idx_raw_complaint_id ON consumer_complaints_raw(complaint_id);"
                )
                logging.info("Created index: idx_raw_complaint_id")
            else:
                logging.info("Index already exists: idx_raw_complaint_id")

            if 'idx_cleaned_complaint_id' not in indexes_cleaned:
                conn.exec_driver_sql(
                    "CREATE INDEX idx_cleaned_complaint_id ON consumer_complaints_cleaned(complaint_id);"
                )
                logging.info("Created index: idx_cleaned_complaint_id")
            else:
                logging.info("Index already exists: idx_cleaned_complaint_id")

    except Exception as e:
        logging.error(f"Failed to create indexes: {e}", exc_info=True)
        sys.exit(1)

# === Main pipeline runner ===
def run_pipeline(step, limit=None): # No changes here, just for context
    try:
        if step in ["all", "ingest"]:
            ensure_tables_and_indexes()
            ensure_metadata_schema() # Check and update metadata table schema
            timed_step("Data Ingestion", lambda: ingestion.run(engine, limit=limit))
        
        if step in ["all", "clean"]:
            timed_step("Data Cleaning", lambda: cleaning.run(engine, limit=limit))
        
        if step in ["all", "insert"]:
            timed_step("Data Insert", lambda: data_insert.run(engine, limit=limit))

        if step in ["all", "model"]:
            if not execute_sql_file(engine, os.path.join("sql", "setup", "create_datamodel_tables.sql"), "Create Data Model Tables"):
                raise Exception("Failed to create data model tables.")
            timed_step("Data Modeling", lambda: modeling.run(engine, limit=limit))

        total_duration = sum(step_durations.values())
        log_db(engine, "Pipeline", "SUCCESS", f"Pipeline completed successfully in {total_duration:.2f} seconds.", duration=total_duration, details=step_durations)

        # Final summary
        logging.info("\nPipeline Summary:")
        for label, duration in step_durations.items():
            logging.info(f"- {label}: {duration} seconds")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        log_db(engine, "Pipeline", "ERROR", f"Pipeline failed with error: {e}")
        sys.exit(1)

# === Entry point ===
if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.step, args.limit)