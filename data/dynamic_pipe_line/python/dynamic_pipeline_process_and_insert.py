"""
Orchestrates the parallel cleaning and transformation of raw complaint data.

This module identifies new records in `consumer_complaints_raw`, processes them in parallel using Pandas, and inserts the cleaned results into `consumer_complaints_cleaned`. It is designed for high performance by using a multiprocessing pool and a staging-table-per-worker pattern to avoid database deadlocks.

The workflow is as follows:
1.  Identifies new raw records (where `cleaned_timestamp` is NULL).
2.  Partitions the workload by `complaint_id` range for parallel processing.
3.  Spawns worker processes, each handling a partition of data.
4.  Each worker reads its partition, cleans it, and writes the results to a unique, temporary staging table.
5.  The main process waits for all workers and then consolidates data from all staging tables into the final `consumer_complaints_cleaned` table in a single transaction.
6.  After consolidation, `cleaned_timestamp` is updated in `consumer_complaints_raw` for all processed records.
"""
import time
import logging
import pandas as pd
import hashlib
from multiprocessing import Pool, cpu_count
from sqlalchemy import text, create_engine, inspect
from pipeline_logger import log_db, setup_logging # Assume these are available
from pipeline_utils import PipelineError, manage_indexes # Assume this is available
import data_standardization_mappings as mappings # Assume this is available

def create_partitions(engine, total_records, num_workers):
    """
    Divides the workload into partitions based on complaint_id ranges using NTILE.

    Args:
        engine: The SQLAlchemy engine for database connectivity.
        total_records (int): The total number of records to partition.
        num_workers (int): The number of partitions to create.

    Returns:
        list: A list of tuples, where each tuple contains the start and end ID for a partition.
    """
    with engine.connect() as conn:
        if total_records == 0:
            return []

        logging.info(f"Calculating {num_workers} partitions for parallel processing using NTILE...")
        partitions = []
        partition_query = text("""
            SELECT
                partition_num,
                MIN(complaint_id) AS start_id,
                MAX(complaint_id) AS end_id
            FROM (
                SELECT complaint_id, NTILE(:num_workers) OVER (ORDER BY complaint_id) as partition_num
                FROM consumer_complaints_raw
                WHERE cleaned_timestamp IS NULL AND complaint_id <= (SELECT MAX(complaint_id) FROM consumer_complaints_raw WHERE cleaned_timestamp IS NULL LIMIT :limit)
            ) AS partitioned_data
            GROUP BY partition_num
            ORDER BY partition_num;
        """)
        results = conn.execute(partition_query, {"num_workers": num_workers, "limit": total_records}).fetchall() # Limit is applied to the subquery
        for i, (part_num, start_id, end_id) in enumerate(results):
            if start_id is not None and end_id is not None:
                partitions.append((start_id, end_id))
    return partitions


def clean_dataframe(df):
    """
    Applies a series of cleaning and standardization rules to a Pandas DataFrame.

    This function handles data validation, type conversion, value standardization via mapping tables, and the creation of a content hash for deduplication. Invalid rows are separated and returned for quarantining.

    Args:
        df (pd.DataFrame): The raw DataFrame to be cleaned.

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: The cleaned and standardized DataFrame.
            - pd.DataFrame or None: A DataFrame of quarantined rows, or None if no rows were quarantined.
    """
    # Explicitly create a copy to avoid SettingWithCopyWarning.
    df = df.copy()

    quarantined_dfs = []

    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.strip()
            
    if 'zip_code' in df.columns:
        null_zip_mask = df['zip_code'].isnull() | (df['zip_code'] == '')
        if null_zip_mask.any():
            quarantined = df[null_zip_mask].copy()
            quarantined['quarantine_reason'] = "Null or empty zip_code"
            quarantined_dfs.append(quarantined)
            df = df[~null_zip_mask]
        
        sanitized_zips = df['zip_code'].str.replace(r'[^\dXx-]', '', regex=True)
        
        valid_zip_pattern = r'^(\d{5}|\d{3}XX|XXXXX|\d{9}|\d{5}-\d{4})$'
        
        invalid_zip_mask = ~sanitized_zips.str.match(valid_zip_pattern, na=False)
        if invalid_zip_mask.any():
            quarantined = df[invalid_zip_mask].copy()
            quarantined['quarantine_reason'] = "Invalid zip code format"
            quarantined_dfs.append(quarantined)
            df = df[~invalid_zip_mask]
        df['zip_code'] = sanitized_zips[~invalid_zip_mask]


    # --- Type Conversion and Standardization ---
    if 'company' in df.columns:
        df['company'] = df['company'].str.title()

    original_date_received = df['date_received']
    df['date_received'] = pd.to_datetime(original_date_received, errors='coerce', format='%Y-%m-%d')

    invalid_date_mask = df['date_received'].isnull() & original_date_received.notnull() & (original_date_received != '')
    if invalid_date_mask.any():
        quarantined = df[invalid_date_mask].copy()
        quarantined['quarantine_reason'] = "Invalid or unparseable date_received"
        quarantined['date_received'] = original_date_received[invalid_date_mask]
        quarantined_dfs.append(quarantined)
        df = df[~invalid_date_mask]

    df['date_received'] = df['date_received'].dt.date

    original_date_sent = df['date_sent_to_company']
    df['date_sent_to_company'] = pd.to_datetime(original_date_sent, errors='coerce', format='%Y-%m-%d')

    invalid_date_mask_sent = df['date_sent_to_company'].isnull() & original_date_sent.notnull() & (original_date_sent != '')
    if invalid_date_mask_sent.any():
        quarantined = df[invalid_date_mask_sent].copy()
        quarantined['quarantine_reason'] = "Invalid or unparseable date_sent_to_company"
        quarantined['date_sent_to_company'] = original_date_sent[invalid_date_mask_sent]
        quarantined_dfs.append(quarantined)
        df = df[~invalid_date_mask_sent]

    df['date_sent_to_company'] = df['date_sent_to_company'].dt.date

    # Timely Response
    valid_timely_response = {'YES', 'NO'}
    invalid_timely_mask = ~df['timely_response'].str.upper().isin(valid_timely_response) & df['timely_response'].notna() & (df['timely_response'] != '')
    if invalid_timely_mask.any():
        quarantined = df[invalid_timely_mask].copy()
        quarantined['quarantine_reason'] = "Invalid value for timely_response"
        quarantined_dfs.append(quarantined)
        df = df[~invalid_timely_mask]
    df['timely_response'] = df['timely_response'].str.upper().map({'YES': '1', 'NO': '0'})

    if 'state_code' in df.columns:
        df['state_code'] = df['state_code'].str.upper().replace(mappings.STATE_MAP)
        
        valid_state_codes = set(mappings.STATE_MAP.values())
        invalid_state_mask = ~df['state_code'].isin(valid_state_codes) & df['state_code'].notnull()
        if invalid_state_mask.any():
            quarantined = df[invalid_state_mask].copy()
            quarantined['quarantine_reason'] = "Invalid or non-US state code"
            quarantined_dfs.append(quarantined)
            df = df[~invalid_state_mask]
        df['state_code'] = df['state_code'].fillna('N/A')

    df['company_public_response_standardized'] = df['company_public_response'].replace('', pd.NA).str.upper().map(mappings.PUB_RESPONSE_MAP).fillna('N/A')
    df['company_public_response'] = df['company_public_response'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['company_response_to_consumer_standardized'] = df['company_response_to_consumer'].replace('', pd.NA).str.upper().map(mappings.COMP_RESPONSE_MAP).fillna('N/A')
    df['company_response_to_consumer'] = df['company_response_to_consumer'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['tags_standardized'] = df['tags'].replace('', pd.NA).str.upper().map(mappings.TAGS_MAP).fillna('General')
    df['tags'] = df['tags'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['consumer_consent_provided_standardized'] = df['consumer_consent_provided'].replace('', pd.NA).str.upper().map(mappings.CONSENT_MAP).fillna('N/A')
    df['consumer_consent_provided'] = df['consumer_consent_provided'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['consumer_disputed_standardized'] = df['consumer_disputed'].replace('', pd.NA).str.upper().map(mappings.DISPUTED_MAP).fillna('N/A')
    df['consumer_disputed'] = df['consumer_disputed'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['consumer_complaint_narrative'] = df['consumer_complaint_narrative'].replace(r'^\s*$', pd.NA, regex=True).fillna('None')

    df['product_standardized'] = df['product'].replace('', pd.NA).str.upper().map(mappings.PRODUCT_MAP).fillna('N/A')
    df['product'] = df['product'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['issue_standardized'] = df['issue'].replace('', pd.NA).str.upper().map(mappings.ISSUE_MAP).fillna('Other/Miscellaneous')
    df['issue'] = df['issue'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['sub_product_standardized'] = df['sub_product'].replace('', pd.NA).str.upper().map(mappings.SUB_PRODUCT_MAP).fillna('N/A')
    df['sub_product'] = df['sub_product'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    df['sub_issue_standardized'] = df['sub_issue'].replace('', pd.NA).str.upper().map(mappings.SUB_ISSUE_MAP).fillna('General/Miscellaneous')
    df['sub_issue'] = df['sub_issue'].replace(r'^\s*$', pd.NA, regex=True).fillna('N/A')

    hash_cols = [
        'date_received', 'product_standardized', 'sub_product_standardized',
        'issue_standardized', 'sub_issue_standardized', 'consumer_complaint_narrative', 'company'
    ]
    df_for_hash = df[hash_cols].fillna('').astype(str)
    
    combined_string_series = df_for_hash[hash_cols[0]].str.cat(df_for_hash[hash_cols[1:]], sep='||')
    df['content_hash'] = combined_string_series.apply(lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest())

    final_cols = [
        'date_received', 'product', 'product_standardized', 'sub_product', 'sub_product_standardized',
        'issue', 'issue_standardized', 'sub_issue', 'sub_issue_standardized', 'consumer_complaint_narrative',
        'company_public_response', 'company', 'state_code', 'zip_code', 'tags', 'tags_standardized',
        'consumer_consent_provided', 'consumer_consent_provided_standardized', 'submitted_via',
        'date_sent_to_company', 'company_response_to_consumer', 'company_response_to_consumer_standardized', 'timely_response',
        'consumer_disputed', 'consumer_disputed_standardized', 'company_public_response_standardized', 'complaint_id',
        'content_hash'
    ]
    df_final = df[[col for col in final_cols if col in df.columns]]

    if quarantined_dfs:
        final_quarantined_df = pd.concat(quarantined_dfs, ignore_index=True)
        return df_final, final_quarantined_df
    else:
        return df_final, None


def processing_worker(args):
    worker_id, start_id, end_id, db_url, batch_size = args
    setup_logging()

    worker_staging_table = f"staging_cleaned_{worker_id}_{int(time.time())}"
    worker_engine = create_engine(db_url)
    total_rows_staged = 0

    logging.info(f"[Processing Worker {worker_id}] Starting partition: IDs {start_id:,} to {end_id:,}")

    query = f"""
        SELECT 
            date_received,
            product,
            sub_product,
            issue,
            sub_issue,
            consumer_complaint_narrative,
            company_public_response,
            company,
            state_code,
            zip_code,
            tags,
            consumer_consent_provided,
            submitted_via,
            date_sent_to_company,
            company_response_to_consumer,
            timely_response,
            consumer_disputed,
            complaint_id
        FROM consumer_complaints_raw
        WHERE complaint_id BETWEEN {start_id} AND {end_id} AND cleaned_timestamp IS NULL
    """
    
    try:
        with worker_engine.connect() as conn:
            with conn.begin():
                chunk_iterator = pd.read_sql_query(sql=query, con=conn, chunksize=batch_size)
                for i, df_chunk in enumerate(chunk_iterator):
                    if df_chunk.empty:
                        continue
                    logging.info(f"[Processing Worker {worker_id}] ...processing batch {i+1} ({len(df_chunk):,} records).")
                    df_cleaned, df_quarantined = clean_dataframe(df_chunk)
                    
                    if not df_cleaned.empty:
                        df_cleaned.to_sql(worker_staging_table, conn, if_exists='append', index=False)
                        total_rows_staged += len(df_cleaned)
                    
                    if df_quarantined is not None and not df_quarantined.empty:
                        quarantine_cols = [c['name'] for c in inspect(conn).get_columns('consumer_complaints_quarantined')]
                        quarantine_cols_in_df = [col for col in df_quarantined.columns if col in quarantine_cols]
                        
                        df_quarantined_final = df_quarantined[quarantine_cols_in_df]
                        df_quarantined_final.to_sql('consumer_complaints_quarantined', conn, if_exists='append', index=False)
                        logging.warning(f"[Processing Worker {worker_id}] ...quarantined {len(df_quarantined):,} records.")
    except Exception as e:
        logging.error(f"[Processing Worker {worker_id}] Failed during processing: {e}", exc_info=True)
        try:
            with worker_engine.begin() as conn_fail:
                conn_fail.execute(text(f"DROP TABLE IF EXISTS `{worker_staging_table}`;"))
            logging.warning(f"[Processing Worker {worker_id}] Cleaned up failed staging table.")
        except:
            pass
        return None
    finally:
        worker_engine.dispose()

    logging.info(f"[Processing Worker {worker_id}] Finished partition. Staged {total_rows_staged:,} records to '{worker_staging_table}'.")
    return worker_staging_table if total_rows_staged > 0 else None

def consolidation_worker(args):
    """
    A worker that consolidates data from one staging table into the final cleaned table.
    It connects, inserts the data, and then drops its assigned staging table.
    """
    table_name, db_url, batch_size = args
    worker_engine = create_engine(db_url)
    total_inserted = 0
    
    logging.info(f"[Consolidation Worker] Consolidating table '{table_name}'...")
    
    try:
        last_id = 0
        while True:
            with worker_engine.begin() as conn:
                inspector = inspect(conn)
                staging_cols_list = [c['name'] for c in inspector.get_columns(table_name)]
                cols_str = ', '.join([f"`{col}`" for col in staging_cols_list])

                insert_sql = text(f"""
                    INSERT IGNORE INTO consumer_complaints_cleaned ({cols_str}) 
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
    A worker that updates the cleaned_timestamp for a given partition of complaint IDs.
    """
    worker_id, start_id, end_id, db_url, batch_size = args
    worker_engine = create_engine(db_url)
    total_updated = 0
    
    logging.info(f"[Timestamp Worker {worker_id}] Updating cleaned_timestamps for IDs {start_id:,} to {end_id:,}")
    last_id = start_id - 1 # Start just before the partition begins
    try:
        while True:
            with worker_engine.begin() as conn:
                update_sql = text("""
                    UPDATE consumer_complaints_raw
                    SET cleaned_timestamp = NOW()
                    WHERE cleaned_timestamp IS NULL
                      AND complaint_id > :last_id
                      AND complaint_id <= :end_id
                    ORDER BY complaint_id
                    LIMIT :batch_size;
                """)
                result = conn.execute(update_sql, {"last_id": last_id, "end_id": end_id, "batch_size": batch_size})
                updated_in_batch = result.rowcount
                total_updated += updated_in_batch
                logging.info(f"[Timestamp Worker {worker_id}] ...updated a batch of {updated_in_batch:,} records.")

                if updated_in_batch == 0 or updated_in_batch < batch_size:
                    break # Last batch was processed

                # Find the new last_id for the next iteration by getting the max ID from the updated batch
                find_last_id_sql = text(f"""
                    SELECT MAX(complaint_id) FROM (
                        SELECT complaint_id FROM consumer_complaints_raw
                        WHERE complaint_id > :last_id AND complaint_id <= :end_id
                        ORDER BY complaint_id LIMIT {updated_in_batch}
                    ) as t;
                """)
                last_id = conn.execute(find_last_id_sql, {"last_id": last_id, "end_id": end_id}).scalar_one()
        logging.info(f"[Timestamp Worker {worker_id}] Marked {total_updated:,} raw records as cleaned.")
    except Exception as e:
        logging.error(f"[Timestamp Worker {worker_id}] Failed to update timestamps: {e}", exc_info=True)
        return 0 # Return 0 on failure
    finally:
        worker_engine.dispose()
        
    return total_updated


def run(engine, limit=None, batch_size=50000):
    """
    Manages the parallel execution of the Stage, Clean, and Insert workflow.

    Args:
        engine: The SQLAlchemy engine for database connectivity.
        limit (int, optional): Max number of records to process. Defaults to all new records.
        batch_size (int, optional): Number of records per batch. Defaults to 10000.

    Raises:
        PipelineError: If the processing pipeline fails.
    """
    try:
        logging.info("Starting parallel processing and insertion...")
        start_time = time.time()

        with engine.connect() as conn:
            id_range_sql = "SELECT MIN(complaint_id), MAX(complaint_id), COUNT(complaint_id) FROM consumer_complaints_raw WHERE cleaned_timestamp IS NULL"
            min_id, max_id, total_records = conn.execute(text(id_range_sql)).first()

        if not total_records or total_records == 0:
            logging.info("No new records to process. Skipping.")
            return
        
        target_process_count = min(total_records, limit) if limit is not None and limit > 0 else total_records
        logging.info(f"Found {total_records:,} new records. Target for this run: {target_process_count:,}.")

        num_workers = min(cpu_count(), 4)
        partitions = create_partitions(engine, target_process_count, num_workers)
        worker_args = [(i, part_start, part_end, engine.url, batch_size) for i, (part_start, part_end) in enumerate(partitions)]

        with Pool(processes=num_workers) as pool:
            worker_staging_tables = pool.map(processing_worker, worker_args)

        worker_staging_tables = [tbl for tbl in worker_staging_tables if tbl]

        total_inserted = 0
        if worker_staging_tables:
            indexes_to_manage = [
                'idx_cleaned_date_received', 'idx_cleaned_date_sent', 'idx_cleaned_product_std',
                'idx_cleaned_sub_product_std', 'idx_cleaned_issue_std', 'idx_cleaned_sub_issue_std',
                'idx_cleaned_company', 'idx_cleaned_state_code', 'idx_cleaned_zip_code',
                'idx_cleaned_submitted_via', 'idx_cleaned_comp_resp_std', 'idx_cleaned_pub_resp_std',
                'idx_cleaned_consent_std', 'idx_cleaned_disputed_std', 'idx_cleaned_tags_std'
            ]

            with manage_indexes(engine, 'consumer_complaints_cleaned', indexes_to_manage):
                logging.info(f"Consolidating data from {len(worker_staging_tables)} worker staging tables IN PARALLEL...")
                try:
                    consolidation_args = [(table, engine.url, batch_size) for table in worker_staging_tables]
                    with Pool(processes=num_workers) as pool:
                        inserted_counts = pool.map(consolidation_worker, consolidation_args)
                    
                    total_inserted = sum(inserted_counts)

                except Exception as e:
                    logging.error(f"Parallel consolidation failed: {e}", exc_info=True)
                    with engine.begin() as conn_cleanup:
                        for table_name in worker_staging_tables:
                            conn_cleanup.execute(text(f"DROP TABLE IF EXISTS `{table_name}`;"))
                    raise

                logging.info(f"Consolidation complete. Total new unique records inserted: {total_inserted:,}")

        details = {
            "total_records_inserted": total_inserted,
            "target_record_count": target_process_count,
            "batch_size_per_worker": batch_size
        }
        
        total_duration = time.time() - start_time
        logging.info(f"Parallel processing and insertion complete in {total_duration:.2f}s.")
        
        if worker_staging_tables:
            logging.info("Starting PARALLEL UPDATE for cleaned_timestamp on raw records...")
            timestamp_args = [(i, part_start, part_end, engine.url, batch_size) for i, (part_start, part_end) in enumerate(partitions)]
            
            with Pool(processes=num_workers) as pool:
                updated_counts = pool.map(timestamp_worker, timestamp_args)
            
            total_marked = sum(updated_counts)
            logging.info(f"Timestamping complete. Total records marked as cleaned: {total_marked:,}")



        log_db(engine, "Process and Insert", "SUCCESS", f"Successfully processed and inserted {total_inserted:,} records.", duration=total_duration, details=details)

    except Exception as e:
        logging.error(f"Unified processing pipeline failed: {e}", exc_info=True)
        raise PipelineError(f"Unified processing failed: {e}")