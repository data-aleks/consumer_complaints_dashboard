import logging
from multiprocessing import Pool, cpu_count
from sqlalchemy import text, create_engine, inspect
# Assuming these are defined elsewhere and work correctly
# from pipeline_logger import log_db, setup_logging
# from pipeline_utils import execute_sql_file, PipelineError 
import time
import os
import uuid
class PipelineError(Exception):
    pass
def log_db(*args, **kwargs):
    logging.info(f"DB Log: {args[1]}: {args[2]} - {args[3]}")
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(process)d - %(levelname)s - %(message)s')
def execute_sql_file(conn, file_path, split_statements=True, params=None, log_prefix=""):
    """
    Reads an SQL file, optionally splits it into individual statements, and executes them against the database.

    Args:
        conn: The SQLAlchemy connection to use.
        file_path (str): The absolute path to the SQL file.
        split_statements (bool): If True, splits the file content by ';' to execute statements individually.
        params (dict, optional): A dictionary of parameters to bind to the SQL statements.
        log_prefix (str): A prefix for logging messages.

    Returns:
        The result object from the execution of the *last* statement in the file, which contains the rowcount.
    """
    logging.info(f"{log_prefix} Executing SQL file: {os.path.basename(file_path)} with params: {params}")
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    format_params = {k: v for k, v in params.items() if isinstance(v, str)} if params else {}
    bind_params = {k: v for k, v in params.items() if k not in format_params} if params else {}
    if format_params:
        sql_content = sql_content.format(**format_params)

    result = None
    if split_statements:
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for statement in statements:
            result = conn.execute(text(statement), bind_params)
    else:
        result = conn.execute(text(sql_content), bind_params)

    return result

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
dimension_script_path = os.path.join(SCRIPT_DIR, "sql", "data_modeling", "populate_dimensions.sql")
fact_script_path = os.path.join(SCRIPT_DIR, "sql", "data_modeling", "populate_facts.sql")

def modeling_worker(args):
    """
    A worker function that models a partition of data and inserts it into a unique staging table.
    This worker iterates through its assigned ID range, processing records in batches
    to keep memory usage low and provide robust, scalable performance.
    """
    worker_id, start_id, end_id, db_url, batch_size, queue_table = args
    worker_engine = create_engine(db_url)
    
    fact_staging_table = f"fact_staging_{worker_id}_{str(uuid.uuid4())[:8]}"
    total_staged_in_worker = 0

    logging.info(f"[Modeling Worker {worker_id}] Starting partition: IDs {start_id:,} to {end_id:,}. Staging table: {fact_staging_table}")

    try:
        with worker_engine.begin() as conn:
            _create_fact_staging_table(conn, fact_staging_table)

        last_id = start_id - 1
        batch_num = 0
        while True:
            try:
                with worker_engine.begin() as conn:
                    batch_num += 1
                    params = {
                        'start_id': start_id,
                        'end_id': end_id,
                        'last_id': last_id,
                        'limit': batch_size,
                        'fact_staging_table': fact_staging_table,
                        'queue_table': queue_table
                    }
                    log_prefix = f"[Modeling Worker {worker_id}, Batch {batch_num}]"
                    
                    execute_sql_file(conn, fact_script_path, split_statements=True, params=params, log_prefix=log_prefix)
                    
                    rows_in_batch = conn.execute(text("SELECT COUNT(*) FROM temp_modeling_batch")).scalar_one()

                    if rows_in_batch == 0:
                        logging.info(f"[Modeling Worker {worker_id}] ...finished final batch. Ending partition processing.")
                        break

                    total_staged_in_worker += rows_in_batch
                    logging.info(f"[Modeling Worker {worker_id}] ...staged batch of {rows_in_batch:,}. Total for worker: {total_staged_in_worker:,}")

                    last_id = conn.execute(text("SELECT MAX(complaint_id) FROM temp_modeling_batch")).scalar_one_or_none()
                    if last_id is None: break
            except Exception as e:
                logging.error(f"[Modeling Worker {worker_id}] Failed during batch processing for IDs > {last_id}: {e}", exc_info=True)
                break
    except Exception as e:
        logging.error(f"[Modeling Worker {worker_id}] An unexpected error occurred: {e}", exc_info=True)
        return None
    finally:
        worker_engine.dispose()
    logging.info(f"[Modeling Worker {worker_id}] Finished partition. Total staged by this worker: {total_staged_in_worker:,}")
    return fact_staging_table if total_staged_in_worker > 0 else None

def _create_fact_staging_table(conn, table_name):
    """Creates a staging table with the same structure as fact_complaints but without foreign keys, which can cause deadlocks."""
    inspector = inspect(conn)
    source_cols = [c['name'] for c in inspector.get_columns('fact_complaints') if c['name'] != 'complaint_fact_key']
    cols_str = ', '.join([f"`{col}`" for col in source_cols])
    
    conn.execute(text(f"CREATE TABLE `{table_name}` AS SELECT {cols_str} FROM fact_complaints LIMIT 0;"))

def consolidation_worker(args):
    """
    A worker that consolidates data from one staging table into the final fact table and then cleans it up.
    """
    table_name, db_url, batch_size = args
    worker_engine = create_engine(db_url)
    total_inserted = 0
    
    logging.info(f"[Consolidation Worker] Consolidating fact table '{table_name}'...")
    
    try:
        last_id = 0
        while True:
            with worker_engine.begin() as conn:
                inspector = inspect(conn)
                staging_cols = [c['name'] for c in inspector.get_columns(table_name) if c['name'] != 'complaint_fact_key']
                cols_str = ', '.join([f"`{col}`" for col in staging_cols])

                insert_sql = text(f"""
                    INSERT IGNORE INTO fact_complaints ({cols_str}) 
                    SELECT {cols_str} FROM `{table_name}`
                    WHERE complaint_id > :last_id
                    ORDER BY complaint_id
                    LIMIT :batch_size;
                """)
                result = conn.execute(insert_sql, {"last_id": last_id, "batch_size": batch_size})
                inserted_in_batch = result.rowcount
                total_inserted += inserted_in_batch
                logging.info(f"[Consolidation Worker] ...inserted batch of {inserted_in_batch:,} rows from '{table_name}'.")

                if inserted_in_batch == 0:
                    break

                last_id = conn.execute(
                    text(f"SELECT MAX(complaint_id) FROM (SELECT complaint_id FROM `{table_name}` WHERE complaint_id > :last_id ORDER BY complaint_id LIMIT :batch_size) AS t"),
                    {"last_id": last_id, "batch_size": batch_size}
                ).scalar_one()

                if last_id is None:
                    break
        
        with worker_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`;"))
        logging.info(f"[Consolidation Worker] Finished consolidating '{table_name}'. Inserted {total_inserted:,} total rows.")
    except Exception as e:
        logging.error(f"[Consolidation Worker] Failed to consolidate {table_name}: {e}", exc_info=True)
        return 0 # Return 0 on failure
    finally:
        worker_engine.dispose()
    return total_inserted

def timestamp_worker(args):
    """
    A worker that updates the modeling_timestamp for a given partition of complaint IDs.
    """
    worker_id, start_id, end_id, db_url, batch_size, _ = args # queue_table is no longer needed
    worker_engine = create_engine(db_url)
    total_updated = 0
    
    logging.info(f"[Timestamp Worker {worker_id}] Updating timestamps for IDs {start_id:,} to {end_id:,}")
    
    try:
        last_id = start_id - 1 # Start just before the partition begins
        while True:
            with worker_engine.begin() as conn:
                update_sql = text(f"""
                    UPDATE consumer_complaints_raw
                    SET modeling_timestamp = NOW()
                    WHERE modeling_timestamp IS NULL
                      AND complaint_id > :last_id
                      AND complaint_id <= :end_id
                    ORDER BY complaint_id
                    LIMIT :batch_size;
                """)
                result = conn.execute(update_sql, {
                    "last_id": last_id, 
                    "end_id": end_id, 
                    "batch_size": batch_size
                })
                updated_in_batch = result.rowcount
                total_updated += updated_in_batch
                logging.info(f"[Timestamp Worker {worker_id}] ...updated a batch of {updated_in_batch:,} records.")

                if updated_in_batch == 0 or updated_in_batch < batch_size:
                    break
                
                # Find the new last_id for the next iteration by getting the max ID from the updated batch
                find_last_id_sql = text(f"""
                    SELECT MAX(complaint_id) FROM (
                        SELECT complaint_id FROM consumer_complaints_raw
                        WHERE complaint_id > :last_id AND complaint_id <= :end_id
                        ORDER BY complaint_id
                        LIMIT {updated_in_batch}
                    ) AS t
                """)
                last_id = conn.execute(find_last_id_sql, {"last_id": last_id, "end_id": end_id}).scalar_one()
                if last_id is None:
                    break
        logging.info(f"[Timestamp Worker {worker_id}] Marked {total_updated:,} raw records as modeled.")
    except Exception as e:
        logging.error(f"[Timestamp Worker {worker_id}] Failed to update timestamps: {e}", exc_info=True)
        return 0 # Return 0 on failure
    finally:
        worker_engine.dispose()
        
    return total_updated

def run(engine, limit=None, batch_size=50000):
    """
    Runs the data modeling process by transforming cleaned data into a star schema.

    This function executes a high-performance, parallel workflow:
    It pre-populates dimension tables to prevent deadlocks, then uses a multiprocessing
    Pool to populate the fact table in parallel.

    Args:
        engine: The SQLAlchemy engine for database connectivity.
        limit (int, optional): The maximum number of records to model in this run. Defaults to all new records.
        batch_size (int, optional): The number of records to process in each modeling batch.
                                    This size is passed to the underlying SQL script. Defaults to 50000.

    Raises:
        PipelineError: If any part of the modeling process fails.
    """
    all_new_records_table = f"temp_all_new_records_{str(uuid.uuid4())[:8]}"
    worker_staging_tables = []
    try:
        total_modeled_count = 0
        logging.info("Starting parallel data modeling process...")
        start_parallel_modeling = time.time()

        num_workers = min(cpu_count(), 4)

        with engine.connect() as conn:
            count_sql = text("""
                SELECT COUNT(c.complaint_id) 
                FROM consumer_complaints_cleaned c
                JOIN consumer_complaints_raw r ON c.complaint_id = r.complaint_id
                WHERE r.modeling_timestamp IS NULL
            """)
            total_records = conn.execute(count_sql).scalar_one_or_none() or 0

        if not total_records or total_records == 0:
            logging.info("No new records to model. Skipping.")
            return

        target_model_count = min(total_records, limit) if limit is not None and limit > 0 else total_records
        logging.info(f"Found {total_records:,} records to model. Target for this run: {target_model_count:,}.")

        logging.info(f"Creating and populating temporary ID queue '{all_new_records_table}'...")
        with engine.begin() as conn:
            create_temp_sql = text(f"""
                CREATE TABLE {all_new_records_table} AS
                SELECT c.complaint_id 
                FROM consumer_complaints_cleaned c
                JOIN consumer_complaints_raw r ON c.complaint_id = r.complaint_id
                WHERE r.modeling_timestamp IS NULL 
                ORDER BY c.complaint_id
                LIMIT :limit;
            """)
            conn.execute(create_temp_sql, {"limit": target_model_count})

            conn.execute(text(f"ALTER TABLE {all_new_records_table} ADD PRIMARY KEY (complaint_id);"))

        logging.info("Pre-populating all dimension tables with new values...")
        with engine.begin() as conn:
            params = {'queue_table': all_new_records_table}
            execute_sql_file(conn, dimension_script_path, split_statements=True, params=params)
        logging.info("Dimension tables pre-populated successfully.")

        logging.info(f"Calculating {num_workers} partitions for parallel modeling...")
        partitions = []
        with engine.connect() as conn:
            partition_query = text(f"""
                SELECT
                    partition_num,
                    MIN(complaint_id) AS start_id,
                    MAX(complaint_id) AS end_id
                FROM (
                    SELECT complaint_id, NTILE(:num_workers) OVER (ORDER BY complaint_id) as partition_num
                    FROM {all_new_records_table}
                ) AS partitioned_data
                GROUP BY partition_num
                ORDER BY partition_num;
            """)
            results = conn.execute(partition_query, {"num_workers": num_workers}).fetchall()
            
            for i, (part_num, part_start_id, part_end_id) in enumerate(results):
                if part_start_id is not None and part_end_id is not None:
                    partitions.append((i, part_start_id, part_end_id, engine.url, batch_size, all_new_records_table))

        logging.info(f"Starting {len(partitions)} parallel modeling worker processes...")
        with Pool(processes=num_workers, initializer=setup_logging) as pool:
            worker_staging_tables = pool.map(modeling_worker, partitions)
        
        worker_staging_tables = [tbl for tbl in worker_staging_tables if tbl]

        total_inserted = 0
        if worker_staging_tables:
            logging.info(f"Consolidating data from {len(worker_staging_tables)} worker fact staging tables IN PARALLEL...")
            try:
                consolidation_args = [(table, engine.url, batch_size) for table in worker_staging_tables]
                with Pool(processes=num_workers, initializer=setup_logging) as pool:
                    inserted_counts = pool.map(consolidation_worker, consolidation_args)
                
                total_inserted = sum(inserted_counts)

            except Exception as e:
                logging.error(f"Parallel fact consolidation failed: {e}", exc_info=True)
                with engine.begin() as conn_cleanup:
                    for table_name in worker_staging_tables:
                        conn_cleanup.execute(text(f"DROP TABLE IF EXISTS `{table_name}`;"))
                raise
            
            logging.info(f"Fact consolidation complete. Total new fact records inserted: {total_inserted:,}")

        total_modeled_count = total_inserted if worker_staging_tables else 0
        total_duration = time.time() - start_parallel_modeling
        overall_rate = total_modeled_count / total_duration if total_duration > 0 else 0
        logging.info(f"Parallel modeling process complete: {total_modeled_count:,} records modeled in {total_duration:.2f}s ({overall_rate:,.0f} records/s).")
        if total_modeled_count > 0:
            logging.info("Starting PARALLEL UPDATE for modeling_timestamp on raw records...")
            timestamp_args = [(p[0], p[1], p[2], engine.url, batch_size, all_new_records_table) for p in partitions]
            
            with Pool(processes=num_workers, initializer=setup_logging) as pool:
                updated_counts = pool.map(timestamp_worker, timestamp_args)
            
            total_marked = sum(updated_counts)
            logging.info(f"Timestamping complete. Total records marked: {total_marked:,}")
            
        details = {
            "total_records_modeled": total_modeled_count,
            "target_record_count": target_model_count,
            "num_workers": num_workers,
            "partitions_created": len(partitions),
            "batch_size_per_worker": batch_size
        }
        log_db(engine, "Data Modeling", "SUCCESS", f"Successfully modeled {total_modeled_count} records.", duration=total_duration, details=details)
    except BaseException as e:
        logging.error(f"Data modeling pipeline failed: {e}", exc_info=True)
        raise PipelineError(f"Data modeling failed: {e}")
    finally:
        logging.info(f"Cleaning up main queue table '{all_new_records_table}'...")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {all_new_records_table};"))