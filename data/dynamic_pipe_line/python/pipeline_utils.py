import logging
import sys
from pipeline_logger import log_db
from sqlalchemy import text, inspect
import os
from contextlib import contextmanager

class PipelineError(Exception):
    """Custom exception for pipeline-specific errors."""
    pass

INDEX_DEFINITIONS = {
    'consumer_complaints_raw': {
        'idx_raw_complaint_id': '(complaint_id)',
        'idx_raw_cleaned_timestamp': '(cleaned_timestamp)',
        'idx_raw_staging_run_id': '(staging_run_id)',
        'idx_raw_modeling_timestamp': '(modeling_timestamp)'
    },
    'consumer_complaints_cleaned': {
        'idx_cleaned_complaint_id': '(complaint_id)',
        'idx_cleaned_content_hash': '(content_hash)',
        'idx_cleaned_date_received': '(date_received)',
        'idx_cleaned_date_sent': '(date_sent_to_company)',
        'idx_cleaned_product_std': '(product_standardized)',
        'idx_cleaned_sub_product_std': '(sub_product_standardized)',
        'idx_cleaned_issue_std': '(issue_standardized)',
        'idx_cleaned_sub_issue_std': '(sub_issue_standardized)',
        'idx_cleaned_company': '(company)', 'idx_cleaned_state_code': '(state_code)', 'idx_cleaned_zip_code': '(zip_code)',
        'idx_cleaned_submitted_via': '(submitted_via)', 'idx_cleaned_comp_resp_std': '(company_response_to_consumer_standardized)',
        'idx_cleaned_pub_resp_std': '(company_public_response_standardized)', 'idx_cleaned_consent_std': '(consumer_consent_provided_standardized)',
        'idx_cleaned_disputed_std': '(consumer_disputed_standardized)', 'idx_cleaned_tags_std': '(tags_standardized)'
    },
}

def execute_sql_file(conn, script_path, split_statements=False, ignore_errors_in=None, params=None, log_prefix=""):
    """
    Executes a SQL script from a file.

    Args:
        conn (Connection): An active SQLAlchemy connection.
        script_path (str): Path to the .sql file.
        split_statements (bool): If True, splits the script by semicolons and runs each statement individually.
        ignore_errors_in (list, optional): A list of substrings. If an error message contains one of these,
                                           it's logged as a warning instead of a critical error. Defaults to None.
        params (dict, optional): A dictionary of parameters to format into the SQL script (e.g., table names).
                                 Defaults to None.
        log_prefix (str, optional): A prefix for log messages to provide context (e.g., worker ID). Defaults to "".

    Returns:
        The SQLAlchemy ResultProxy from the last executed statement, or None if no statements were run.
    """
    try:
        with open(script_path, "r", encoding="utf-8") as file:
            sql_script = file.read()

        if params:
            sql_script = sql_script.format(**params)

        if not sql_script.strip():
            logging.warning(f"SQL script is empty: {script_path}. Skipping.")
            return

        statements = [sql_script]
        if split_statements:
            statements = [s for s in sql_script.split(';') if s.strip()]

        result = None
        for stmt in statements:
            try:
                result = conn.execute(text(stmt), {})
            except Exception as e: # Catch execution error for a single statement
                # Check if this is a controlled, skippable error (e.g., "table already exists")
                if ignore_errors_in and any(keyword in str(e) for keyword in ignore_errors_in):
                    logging.warning(f"Skipping controlled error in {script_path}: {e}")
                else:
                    raise  # Re-raise the exception if it's not a controlled error.
        
        log_message = f"Successfully executed SQL script: {script_path}"
        logging.info(f"{log_prefix} {log_message}" if log_prefix else log_message)
        return result

    except Exception as e:
        logging.error(f"Error executing SQL file '{script_path}': {e}", exc_info=True)
        raise PipelineError(f"Failed to execute SQL script {script_path}: {e}")

def ensure_tables_exist(engine):
    """
    Ensures all required database tables exist by executing setup SQL scripts.
    
    This function iterates through a predefined dictionary of setup scripts and executes them.
    This function is the single source of truth for creating and migrating schema.
    """
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    setup_scripts = {
        "consumer_complaints_raw": os.path.join(script_dir, "sql", "setup", "create_raw_data_table.sql"),
        "consumer_complaints_cleaned": os.path.join(script_dir, "sql", "setup", "create_cleaned_data_table.sql"),
        "ingestion_metadata": os.path.join(script_dir, "sql", "setup", "create_ingestion_metadata_table.sql"),
        "pipeline_logs": os.path.join(script_dir, "sql", "setup", "create_pipeline_logs_table.sql"),
        "star_schema": os.path.join(script_dir, "sql", "setup", "create_datamodel_tables.sql"),
        "consumer_complaints_quarantined": os.path.join(script_dir, "sql", "setup", "create_consumer_complaints_quarantined_table.sql"),
    }

    logging.info("Executing all setup scripts to ensure database schema is up-to-date...")
    with engine.begin() as conn:
        for name, script_path in setup_scripts.items():
            logging.info(f"Running setup script: {name} ({os.path.basename(script_path)})")
            execute_sql_file(conn, script_path, split_statements=True, ignore_errors_in=['already exists', 'Duplicate column name'])

def ensure_indexes_exist(engine):
    """
    Programmatically creates indexes on tables if they are missing.
    
    This function defines a desired state for indexes on key tables, inspects the database, and creates any missing indexes, making the setup process more
    robust and idempotent.
    """
    inspector = inspect(engine)
    with engine.begin() as conn:
        for table_name, indexes in INDEX_DEFINITIONS.items():
            try:
                existing_indexes = [idx['name'] for idx in inspector.get_indexes(table_name)]
                for index_name, column_def in indexes.items():
                    if index_name not in existing_indexes:
                        logging.info(f"Creating index '{index_name}' on table '{table_name}'...")
                        conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} {column_def};"))
                        logging.info(f"Created index: {index_name}")
            except Exception as e:
                logging.warning(f"Could not check or create indexes for table '{table_name}'. It might not exist yet. Error: {e}")

@contextmanager
def manage_indexes(engine, table_name, index_names_to_manage):
    """
    A context manager to temporarily drop specified indexes for bulk operations and reliably restore them afterward.

    This is a key performance optimization for write-heavy operations.

    Args:
        engine: The SQLAlchemy engine.
        table_name (str): The name of the table whose indexes are being managed.
        index_names_to_manage (list): A list of index names to drop and recreate.
    """
    index_definitions = {name: INDEX_DEFINITIONS.get(table_name, {}).get(name) for name in index_names_to_manage}

    logging.info(f"Temporarily dropping {len(index_definitions)} indexes on table '{table_name}' for bulk operation...")
    with engine.begin() as conn:
        for index_name in index_definitions:
            conn.execute(text(f"DROP INDEX {index_name} ON {table_name};"))
    
    try:
        yield # The code inside the 'with' block runs here
    finally:
        logging.info(f"Recreating {len(index_definitions)} indexes on table '{table_name}'...")
        with engine.begin() as conn:
            for index_name, column_def in index_definitions.items():
                if column_def:
                    conn.execute(text(f"CREATE INDEX {index_name} ON {table_name} {column_def};"))
        logging.info("Index recreation complete.")