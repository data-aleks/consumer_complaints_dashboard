import logging
import sys
from sqlalchemy import text
import json
from sqlalchemy.engine import Engine, Connection # Explicitly import types for clarity and checks

def setup_logging():
    """
    Configures the root logger for the entire application.

    This function sets up a centralized logging system that writes logs to both
    a file (`pipeline_run.log`) and the console (stdout). It clears any existing
    handlers to prevent duplicate log entries.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    log_file = "pipeline_run.log"
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8') # 'a' for append
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler(sys.stdout) # Log to console
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

def log_db(engine_or_conn, step, status, message, duration=None, details=None):
    """
    Logs a pipeline action to both the standard logger and the `pipeline_logs` database table.

    This function is flexible and can operate in two modes for database logging:
    1.  If an `Engine` is passed, it creates a new connection and transaction to
        log the message atomically.
    2.  If a `Connection` is passed, it uses the existing connection, allowing the
        log message to be part of the caller's ongoing transaction.

    Args:
        engine_or_conn (Engine | Connection): The SQLAlchemy Engine or a live Connection.
        step (str): The name of the pipeline step being logged (e.g., 'Ingestion').
        status (str): The status of the action (e.g., 'SUCCESS', 'ERROR', 'INFO').
        message (str): A descriptive message.
        duration (float, optional): The duration of the action in seconds. Defaults to None.
        details (dict, optional): A dictionary of extra details to be stored as JSON. Defaults to None.
    """
    log_message = f"{step} | {status.upper()} | {message}"
    if status.lower() == 'error':
        logging.error(log_message)
    else:
        logging.info(log_message)

    try:
        if isinstance(engine_or_conn, Engine):
            with engine_or_conn.begin() as conn:
                _execute_log_insert(conn, step, status, message, duration, details)
        elif isinstance(engine_or_conn, Connection):
            _execute_log_insert(engine_or_conn, step, status, message, duration, details)
        else:
            logging.error("Invalid object passed to log_db: must be SQLAlchemy Engine or Connection.")

    except Exception as e:
        logging.error(f"Failed to write log to database: {e}", exc_info=True)


def _execute_log_insert(conn, step, status, message, duration, details):
    """
    A helper function to execute the database insert for a log entry.

    Args:
        conn (Connection): An active SQLAlchemy Connection.
        All other arguments are passed from `log_db`.
    """
    stmt = text("""
        INSERT INTO pipeline_logs (pipeline_step, status, message, duration_seconds, details)
        VALUES (:step, :status, :message, :duration, :details)
    """)
    conn.execute(stmt, {
        "step": step, 
        "status": status, 
        "message": message,
        "duration": duration, 
        "details": json.dumps(details) if details else None
    })