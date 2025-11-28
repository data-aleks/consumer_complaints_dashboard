"""
Main entry point and orchestrator for the Consumer Complaints ETL pipeline.

This script manages the execution of the entire ETL process. It handles:
- Parsing command-line arguments to control pipeline execution.
- Loading database credentials from a secure environment file.
- Creating a pooled database connection engine for performance.
- Orchestrating the execution of the ingestion, processing, and modeling steps.
- Centralized timing and logging for each pipeline stage.
"""
import logging
import sys
import time
import argparse
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.pool import QueuePool
import os
import dynamic_pipeline_data_ingestion as ingestion
import dynamic_pipeline_process_and_insert as process_and_insert
from pipeline_logger import log_db, setup_logging
from pipeline_utils import ensure_tables_exist, ensure_indexes_exist
import dynamic_pipeline_data_modeling as modeling
from dotenv import load_dotenv

# Load database configuration from a .env file for security and portability.
load_dotenv(dotenv_path=".db_config.env")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# --- Database Engine and Connection Pool Configuration ---
# A connection pool is used to manage database connections efficiently, reducing the
# overhead of establishing a new connection for every database operation.
POOL_SIZE = 5
MAX_OVERFLOW = 10
POOL_TIMEOUT = 30
POOL_RECYCLE_SECONDS = 3600

engine = create_engine(
    connection_string,
    connect_args={"local_infile": 1},
    poolclass=QueuePool,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_pre_ping=True,  # Checks connection validity before use, preventing errors from stale connections.
    pool_recycle=POOL_RECYCLE_SECONDS,  # Automatically replaces connections after 1 hour to prevent timeouts.
    pool_timeout=POOL_TIMEOUT,  # Max time to wait for a connection from the pool.
    pool_reset_on_return='rollback',  # Ensures transactions are rolled back when a connection is returned.
    echo=False
)

# Initialize the centralized logging system.
setup_logging()
step_durations = {}

def parse_args():
    """
    Configures and parses command-line arguments for controlling the pipeline.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Run CFPB ETL pipeline")
    parser.add_argument(
        "--step",
        choices=["all", "ingest", "process", "model"],
        default="all",
        help="Which pipeline step to run"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of records to ingest (for development)."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=100000,
        help="Set the batch size for processing steps like staging and cleaning."
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip the initial database setup and migration checks for faster execution."
    )
    return parser.parse_args()

def timed_step(label, func):
    """
    A decorator-like function that wraps a pipeline step to time its execution.

    Args:
        label (str): The name of the step, used for logging.
        func (callable): The function representing the pipeline step to execute.
    """
    logging.info(f"--- {label} started ---")
    start = time.time()
    func()
    duration = round(time.time() - start, 2)
    step_durations[label] = duration
    logging.info(f"--- {label} completed in {duration} seconds ---")

def initial_setup():
    """
    Ensures all database tables, indexes, and schema migrations are in place.
    Called once at the start of the pipeline run unless skipped.
    """
    logging.info("Starting database initial setup and migration check...")
    ensure_tables_exist(engine)
    ensure_indexes_exist(engine)
    logging.info("Database setup and migration check complete.")

def run_pipeline(step, limit=None, batch_size=100000, skip_setup=False):
    """
    The main orchestrator for the ETL pipeline.

    Args:
        step (str): The pipeline step to run ('all', 'ingest', 'process', 'model').
        limit (int, optional): Limits the number of records to process.
        batch_size (int, optional): The size of batches for processing steps.
        skip_setup (bool): If True, skips the initial database setup checks.
    """
    pipeline_start_time = time.time()
    pipeline_succeeded = False
    try:
        if not skip_setup:
            timed_step("Initial DB Setup", initial_setup)

        if step in ["all", "ingest"]:
            timed_step("Data Ingestion", lambda: ingestion.run(engine, limit=limit, batch_size=batch_size))

        if step in ["all", "process"]:
            timed_step("Process and Insert", lambda: process_and_insert.run(engine, limit=limit, batch_size=batch_size))

        if step in ["all", "model"]:
            timed_step("Data Modeling", lambda: modeling.run(engine, limit=limit, batch_size=batch_size))

        pipeline_succeeded = True
    except BaseException as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        duration_on_fail = time.time() - pipeline_start_time
        log_db(engine, "Pipeline", "ERROR", f"Pipeline failed with error: {e}", duration=duration_on_fail, details=step_durations)
        sys.exit(1)
    finally:
        if pipeline_succeeded:
            total_duration = time.time() - pipeline_start_time
            log_db(engine, "Pipeline", "SUCCESS", f"Pipeline completed successfully in {total_duration:.2f} seconds.", duration=total_duration, details=step_durations)
            logging.info("\nPipeline Summary:")
            for label, duration in step_durations.items():
                logging.info(f"- {label}: {duration} seconds")

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.step, args.limit, args.batch_size, args.skip_setup)