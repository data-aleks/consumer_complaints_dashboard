import logging
import sys
from pipeline_logger import log_db

def execute_sql_file(engine, script_path, label, incremental_clause=None, limit=None):
    """
    Executes a SQL script which can contain multiple statements.
    Handles incremental processing and record limits.
    
    Args:
        incremental_clause: For UPDATE statements, use "AND cleaned_timestamp IS NULL"
                           For SELECT/INSERT statements, use "WHERE cleaned_timestamp IS NULL" or "AND c.modeling_timestamp IS NULL"
    """
    try:
        with open(script_path, "r", encoding="utf-8") as file:
            sql_template = file.read()

        # Replace placeholders
        # For cleaning scripts: incremental_clause should be "AND cleaned_timestamp IS NULL" 
        # For fact tables: incremental_clause should be "AND c.modeling_timestamp IS NULL"
        if incremental_clause:
            # If the clause starts with WHERE, it's for INSERT statements
            # If it starts with AND, it's for UPDATE statements or additional WHERE conditions
            sql_template = sql_template.replace("{incremental_clause}", incremental_clause)
            sql_template = sql_template.replace("{incremental_where_clause}", incremental_clause)
        else:
            sql_template = sql_template.replace("{incremental_clause}", "")
            sql_template = sql_template.replace("{incremental_where_clause}", "")
            
        sql_template = sql_template.replace("{limit_clause}", f"LIMIT {limit}" if limit else "")

        with engine.begin() as conn:
            # Split statements by semicolon and execute them
            for stmt in sql_template.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.exec_driver_sql(stmt)
        logging.info(f"Executed: {label}")
        return True
    except Exception as e:
        logging.error(f"Error in {label}: {e}", exc_info=True)
        log_db(engine, label, "ERROR", str(e))
        return False