import logging
from sqlalchemy import text
import json

def log_db(engine, step, status, message, duration=None, details=None):
    """
    Logs a pipeline action to both the standard logger and the `pipeline_logs` database table.

    Args:
        engine: The SQLAlchemy engine for database connection.
        step (str): The name of the pipeline step or action.
        status (str): The status of the action (e.g., 'SUCCESS', 'ERROR', 'INFO').
        message (str): A descriptive message.
        duration (float, optional): The duration of the action in seconds.
        details (dict, optional): A dictionary of extra details to be stored as JSON.
    """
    # Also log to the standard logger to see output in console/files
    log_message = f"{step} | {status.upper()} | {message}"
    if status.lower() == 'error':
        logging.error(log_message, exc_info=False) # Set exc_info to False to avoid double printing tracebacks
    else:
        logging.info(log_message)

    try:
        with engine.begin() as conn:
            stmt = text("""
                INSERT INTO pipeline_logs (pipeline_step, status, message, duration_seconds, details)
                VALUES (:step, :status, :message, :duration, :details)
            """)
            conn.execute(stmt, {"step": step, "status": status, "message": message,
                                "duration": duration, "details": json.dumps(details) if details else None})
    except Exception as e:
        # If logging to DB fails, log this failure to the standard logger and continue
        logging.error(f"Failed to write log to database: {e}")