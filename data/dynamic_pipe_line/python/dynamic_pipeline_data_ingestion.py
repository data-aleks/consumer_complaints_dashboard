"""
Handles the data ingestion step of the ETL pipeline.

This module is optimized for high-speed, low-memory bulk loading by streaming data to disk and using database-native `LOAD DATA` commands. It also includes logic to prevent re-ingestion of unchanged source files.
"""
from sqlalchemy import text
from datetime import datetime
import requests
import csv
import zipfile
import io
import time
import logging
import uuid
import hashlib
import os
import re
import tempfile
from contextlib import contextmanager
from pipeline_utils import PipelineError, manage_indexes


# Configuration for the data ingestion pipeline
source_url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
source_file_name = "complaints.csv"
local_data_dir = "data"
local_zip_path = os.path.join(local_data_dir, "complaints.csv.zip")
ingestion_date = datetime.today().date()


def compute_file_hash(file_path):
    """Computes the SHA256 hash of a file efficiently by reading it in chunks."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_last_ingestion_metadata(engine):
    """Retrieves the file hash and last modified date from the latest ingestion record."""
    query = "SELECT file_hash, last_modified_date FROM ingestion_metadata ORDER BY ingested_at DESC LIMIT 1"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).first()
            if result and result.last_modified_date:
                return result.file_hash, result.last_modified_date.date()
            return None, None
    except Exception as e:
        logging.warning(f"Could not retrieve last ingestion metadata: {e}")
        return None, None

def record_ingestion_metadata(engine, file_hash, row_count, max_id, last_modified_date):
    """Records metadata about the completed ingestion process into the database."""
    insert_sql = text("""
        INSERT INTO ingestion_metadata (source_file_name, file_hash, row_count, max_complaint_id, last_modified_date)
        VALUES (:filename, :file_hash, :row_count, :max_id, :last_modified)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, {
                "filename": source_file_name,
                "file_hash": file_hash,
                "row_count": row_count,
                "max_id": max_id,
                "last_modified": last_modified_date
            })
    except Exception as e:
        logging.error(f"Failed to record ingestion metadata: {e}", exc_info=True)
        raise

@contextmanager
def temporary_innodb_settings(engine):
    """A context manager to temporarily set InnoDB configurations for maximum bulk load speed.
    
    WARNING: This modifies GLOBAL database settings (`innodb_flush_log_at_trx_commit`)
    which affects all connections. It should only be used during controlled maintenance
    or ingestion windows. It significantly speeds up bulk inserts by reducing disk I/O
    at the cost of ACID compliance during the operation.
    """
    logging.info("Optimizing InnoDB settings for **bulk load speed**.")
    original_flush_log = None
    original_unique_checks = None
    try:
        with engine.begin() as conn:
            logging.warning("Temporarily modifying GLOBAL variable 'innodb_flush_log_at_trx_commit'. This will affect all database connections.")
            original_flush_log = conn.execute(text("SELECT @@GLOBAL.innodb_flush_log_at_trx_commit")).scalar()
            original_unique_checks = conn.execute(text("SELECT @@SESSION.unique_checks")).scalar()
            
            conn.execute(text("SET GLOBAL innodb_flush_log_at_trx_commit = 0;"))
            conn.execute(text("SET SESSION unique_checks = 0;"))
        yield
    finally:
        logging.info("Restoring critical InnoDB settings.")
        try:
            with engine.begin() as conn:
                if original_flush_log is not None:
                    conn.execute(text(f"SET GLOBAL innodb_flush_log_at_trx_commit = {original_flush_log};"))
                if original_unique_checks is not None:
                    conn.execute(text(f"SET SESSION unique_checks = {original_unique_checks};"))
        except Exception as e:
            logging.critical(f"CRITICAL: Failed to restore InnoDB settings. Manual intervention required. Error: {e}")

def _perform_bulk_load(engine, local_zip_path, limit):
    """Extracts, sanitizes to a temporary disk file, and bulk loads data into the database."""
    start_ingest_time = time.time()
    staging_run_id = str(uuid.uuid4()) # Generate a unique ID for this ingestion run.
    total_processed_count = 0
    header = []
    quarantined_rows = []

    with tempfile.TemporaryDirectory() as temp_dir:
        extracted_csv_path = os.path.join(temp_dir, 'sanitized_complaints.csv')
        
        try:
            with open(extracted_csv_path, 'w', encoding='utf-8', newline='') as temp_csv_file:
                writer = csv.writer(temp_csv_file)
                
                logging.info(f"[Ingestion] Extracting and sanitizing CSV to temporary disk file...")
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    csv_filename_in_zip = [f for f in zip_ref.namelist() if f.endswith('.csv')][0]
                    with zip_ref.open(csv_filename_in_zip, 'r') as csv_file:
                        text_stream = io.TextIOWrapper(csv_file, encoding='utf-8')
                        reader = csv.reader(text_stream)
                        
                        header = next(reader)
                        writer.writerow(header)
                        num_columns = len(header)
                        complaint_id_index = header.index('Complaint ID')
                        
                        for i, row in enumerate(reader):
                            if limit is not None and i >= limit:
                                logging.info(f"[Ingestion] Reached specified limit of {limit} records during sanitation.")
                                break
                            if len(row) == num_columns:
                                complaint_id = row[complaint_id_index]
                                if complaint_id and complaint_id.isdigit():
                                    writer.writerow(row)
                                else:
                                    quarantined_rows.append({
                                        "complaint_id": complaint_id or 'UNKNOWN',
                                        "quarantine_reason": f"Invalid or missing complaint_id: '{complaint_id}'"
                                    })
                            else:
                                quarantined_rows.append({
                                    "complaint_id": row[complaint_id_index] if len(row) > complaint_id_index else 'UNKNOWN',
                                    "quarantine_reason": f"Incorrect column count: expected {num_columns}, got {len(row)}"
                                })
        except Exception as e:
            logging.error(f"Sanitization or file extraction failed: {e}")
            raise PipelineError(f"Sanitization or file extraction failed: {e}")

        temp_staging_table = f"ingestion_staging_temp_{int(time.time())}"
        
        with engine.begin() as conn:
            logging.info(f"[Ingestion] Creating temporary staging table: {temp_staging_table}")
            conn.execute(text(f"CREATE TEMPORARY TABLE {temp_staging_table} LIKE consumer_complaints_raw;"))

            if quarantined_rows:
                logging.warning(f"[Ingestion] Quarantining {len(quarantined_rows):,} records due to sanitation failure.")
                quarantine_sql = text("""
                    INSERT INTO consumer_complaints_quarantined (complaint_id, quarantine_reason)
                    VALUES (:complaint_id, :quarantine_reason)
                """)
                try:
                    conn.execute(quarantine_sql, quarantined_rows)
                except Exception as q_e:
                    logging.error(f"Failed to insert records into quarantine table: {q_e}")

            sql_safe_path = extracted_csv_path.replace('\\', '\\\\') 
            clean_header = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('-', '_')) for c in header]
            clean_header = ['state_code' if c == 'state' else c for c in clean_header]
            
            at_vars_str = ', '.join([f"@{col}" for col in clean_header])
            set_clause = ', '.join([f"`{col}` = @{col}" for col in clean_header])
            
            load_sql = text(f"""
                LOAD DATA LOCAL INFILE '{sql_safe_path}'
                IGNORE INTO TABLE {temp_staging_table}
                FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                LINES TERMINATED BY '\r\n' 
                IGNORE 1 LINES
                ({at_vars_str})
                SET {set_clause};
            """)
            
            logging.info(f"[Ingestion] Executing LOAD DATA LOCAL INFILE from disk...")
            result = conn.execute(load_sql)
            staged_count = result.rowcount
            logging.info(f"[Ingestion] Bulk load to temporary table complete. Staged {staged_count:,} records.")

            logging.info("[Ingestion] Inserting new unique records from staging table into consumer_complaints_raw...")
            qualified_cols_str = ', '.join([f"s.`{col}`" for col in clean_header])
            cols_str = ', '.join([f"`{col}`" for col in clean_header])
            
            metadata_cols = "ingestion_date, source_file_name, staging_run_id"
            metadata_values = "CURRENT_DATE(), :source_file, :run_id"

            insert_sql = text(f"""
                INSERT INTO consumer_complaints_raw ({cols_str}, {metadata_cols}) 
                SELECT {qualified_cols_str}, {metadata_values} FROM {temp_staging_table} s
                LEFT JOIN consumer_complaints_raw r ON s.complaint_id = r.complaint_id
                WHERE r.complaint_id IS NULL;
            """)
            insert_result = conn.execute(insert_sql, {
                "source_file": source_file_name,
                "run_id": staging_run_id
            })
            total_processed_count = insert_result.rowcount
            logging.info(f"[Ingestion] Successfully inserted {total_processed_count:,} new records.")

        total_duration = time.time() - start_ingest_time
        return total_processed_count

def _handle_file_download(engine, remote_last_modified, last_known_server_date):
    """Handles the logic for downloading the source file if it's new or updated."""
    should_download = True
    if os.path.exists(local_zip_path):
        local_mod_time = datetime.fromtimestamp(os.path.getmtime(local_zip_path)).date()
        
        if local_mod_time == datetime.today().date():
            logging.info("Local file was already downloaded today. Skipping download.")
            if not zipfile.is_zipfile(local_zip_path):
                logging.warning("Existing file is corrupted. Deleting and re-downloading.")
                os.remove(local_zip_path)
            else:
                should_download = False
        elif last_known_server_date and last_known_server_date >= remote_last_modified.date():
             logging.info("Local file is up-to-date per database metadata. Skipping download.")
             should_download = False

    if should_download:
        logging.info(f"New or updated file available. Remote last modified: {remote_last_modified}.")
        start_download = time.time()
        try:
            response = requests.get(source_url, stream=True)
            response.raise_for_status()
            with open(local_zip_path, 'wb') as f:
                for data in response.iter_content(chunk_size=8192):
                    f.write(data)
            duration = time.time() - start_download
            size_mb = os.path.getsize(local_zip_path) / (1024 * 1024)
            logging.info(f"Downloaded {size_mb:.2f} MB in {duration:.2f}s.")
        except requests.RequestException as e:
            raise PipelineError(f"Failed to download file: {e}")

def run(engine, limit=None, batch_size=50000): 
    """Orchestrates the end-to-end data ingestion pipeline for consumer complaints."""
    os.makedirs(local_data_dir, exist_ok=True)

    try:
        logging.info("[Ingestion] Checking remote file modification date...")
        head_response = requests.head(source_url)
        head_response.raise_for_status()
        remote_last_modified_str = head_response.headers.get('Last-Modified')
        remote_last_modified = datetime.strptime(remote_last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')

        last_hash, last_known_server_date = get_last_ingestion_metadata(engine)
        _handle_file_download(engine, remote_last_modified, last_known_server_date)

        file_hash = compute_file_hash(local_zip_path)
        if file_hash == last_hash:
            logging.info("[Ingestion] No changes detected in source file. Skipping ingestion.")
            return

        indexes_to_manage = ['idx_raw_cleaned_timestamp', 'idx_raw_staging_run_id', 'idx_raw_modeling_timestamp']
        
        with temporary_innodb_settings(engine):
            with manage_indexes(engine, 'consumer_complaints_raw', indexes_to_manage):
                total_processed_count = _perform_bulk_load(engine, local_zip_path, limit) 

        with engine.connect() as conn:
            max_id = conn.execute(text("SELECT MAX(complaint_id) FROM consumer_complaints_raw")).scalar_one_or_none() or 0
        logging.info(f"[Ingestion] Max Complaint ID after ingestion: {max_id:,}")
        
        record_ingestion_metadata(engine, file_hash, total_processed_count, max_id, remote_last_modified.replace(tzinfo=None))
        logging.info(f"[Ingestion] Successfully recorded ingestion metadata for file hash: {file_hash}")

    except Exception as e:
        logging.error(f"Ingestion pipeline failed: {e}", exc_info=True)
        raise PipelineError(f"Ingestion failed: {e}")