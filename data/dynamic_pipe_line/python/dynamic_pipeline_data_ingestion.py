import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import requests
import zipfile
import io
import time
from tqdm import tqdm
import logging
from dotenv import load_dotenv
import hashlib
from pipeline_logger import log_db
import os

# === CONFIG ===
source_url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
source_file_name = "complaints.csv"
local_data_dir = "data"
local_zip_path = os.path.join(local_data_dir, "complaints.csv.zip")
ingestion_date = datetime.today().date()

# Load DB credentials from .env
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

def compute_file_hash(file_path):
    """Compute SHA256 hash of a file efficiently without loading entire file into memory."""
    hasher = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_last_ingestion_metadata(engine):
    query = "SELECT file_hash, last_modified_date FROM ingestion_metadata ORDER BY ingested_at DESC LIMIT 1"
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).first()
            return result if result else (None, None)
    except Exception as e:
        logging.warning(f"Could not retrieve last ingestion metadata: {e}")
        return (None, None)

def record_ingestion_metadata(engine, file_hash, row_count, max_id, last_modified_date):
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

# === MAIN RUN FUNCTION ===
def run(engine, limit=None):
    try:
        os.makedirs(local_data_dir, exist_ok=True)

        # --- Remote file check and download ---
        logging.info("Checking remote file modification date...")
        head_response = requests.head(source_url)
        head_response.raise_for_status()
        remote_last_modified_str = head_response.headers.get('Last-Modified')
        remote_last_modified = datetime.strptime(remote_last_modified_str, '%a, %d %b %Y %H:%M:%S %Z')

        _, last_known_server_date = get_last_ingestion_metadata(engine)

        should_download = True
        if os.path.exists(local_zip_path):
            # Check 1: Was the local file downloaded today?
            local_mod_time = datetime.fromtimestamp(os.path.getmtime(local_zip_path)).date()
            if local_mod_time == datetime.today().date():
                logging.info("Local file was already downloaded today. Skipping download.")
                log_db(engine, "Download Skipped", "SUCCESS", "File already downloaded today.")
                # --- Validate if the existing file is a valid zip ---
                if not zipfile.is_zipfile(local_zip_path):
                    logging.warning("Existing file is corrupted or not a zip file. Deleting and re-downloading.")
                    log_db(engine, "File Validation", "ERROR", "Corrupted local zip file found. Re-downloading.")
                    os.remove(local_zip_path)
                    should_download = True
                else:
                    should_download = False
            # Check 2: Is the local file up-to-date based on server metadata?
            elif last_known_server_date and last_known_server_date.replace(tzinfo=remote_last_modified.tzinfo) >= remote_last_modified:
                logging.info("Local file is up-to-date based on last successful ingestion. Skipping download.")
                log_db(engine, "Download Skipped", "SUCCESS", "Local file is current per database metadata.")
                should_download = False

        if should_download:
            logging.info(f"New or updated file available. Remote file last modified on {remote_last_modified_str}.")
            logging.info("Starting download...")
            start_download = time.time()

            response = requests.get(source_url, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024

            with open(local_zip_path, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading ZIP") as pbar:
                for data in response.iter_content(block_size):
                    f.write(data)
                    pbar.update(len(data))

            end_download = time.time()
            download_time = round(end_download - start_download, 2)
            downloaded_mb = os.path.getsize(local_zip_path) / 1024 / 1024
            logging.info(f"Downloaded {downloaded_mb:.2f} MB in {download_time} seconds.")
            log_db(engine, "Download ZIP", "SUCCESS", f"Downloaded {downloaded_mb:.2f} MB", duration=download_time)

        # --- Hash check against database ---
        file_hash = compute_file_hash(local_zip_path)
        with open(local_zip_path, 'rb') as f:
            zip_bytes = io.BytesIO(f.read())

        last_hash, _ = get_last_ingestion_metadata(engine)
        if file_hash == last_hash:
            logging.info("No changes detected in source file. Skipping ingestion.")
            log_db(engine, "Ingestion Skipped", "SUCCESS", "File hash is identical to the last ingested version.")
            return

        logging.info("Extracting CSV...")
        with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
            csv_filename = [f for f in zip_ref.namelist() if f.endswith('.csv')][0]
            
            total_new_records = 0
            max_complaint_id = 0
            
            with zip_ref.open(csv_filename) as csv_file:
                # Process the CSV in chunks to handle large files
                chunk_size_read = 50000
                chunk_size_write = 10000
                
                with pd.read_csv(csv_file, chunksize=chunk_size_read, low_memory=False) as reader:
                    for i, df_chunk in enumerate(tqdm(reader, desc="Processing CSV Chunks")):
                        # --- Sanitize and Transform each chunk ---
                        df_chunk.columns = [col.lower().replace(' ', '_').replace('?', '').replace('-', '_') for col in df_chunk.columns]
                        if 'state' in df_chunk.columns:
                            df_chunk.rename(columns={'state': 'state_code'}, inplace=True)
                        
                        df_chunk["ingestion_date"] = ingestion_date
                        df_chunk["source_file_name"] = source_file_name
                        df_chunk["complaint_id"] = pd.to_numeric(df_chunk["complaint_id"], errors="coerce")

                        # Update max ID for metadata
                        chunk_max_id = df_chunk["complaint_id"].dropna().astype(int).max()
                        if chunk_max_id > max_complaint_id:
                            max_complaint_id = chunk_max_id

                        # --- Deduplication for each chunk ---
                        potential_ids = df_chunk["complaint_id"].dropna().astype(int).tolist()
                        existing_ids_set = set()
                        if potential_ids:
                            query = text("SELECT complaint_id FROM consumer_complaints_raw WHERE complaint_id IN :ids")
                            with engine.connect() as conn:
                                result = conn.execute(query, {"ids": potential_ids})
                                existing_ids_set = {row[0] for row in result}

                        df_new = df_chunk[~df_chunk["complaint_id"].isin(existing_ids_set)]

                        # --- Ingest new records from the chunk ---
                        if not df_new.empty:
                            logging.info(f"Chunk {i+1}: Found {len(df_new)} new records to ingest.")
                            df_new.to_sql("consumer_complaints_raw", con=engine, if_exists="append", index=False, chunksize=chunk_size_write)
                            total_new_records += len(df_new)
                        else:
                            logging.info(f"Chunk {i+1}: No new records in this chunk.")

                        # Handle user-defined limit
                        if limit is not None and total_new_records >= limit:
                            logging.warning(f"Reached user-defined limit of {limit} records. Stopping ingestion.")
                            break

        log_db(engine, "Ingest to MySQL", "SUCCESS", f"Ingested a total of {total_new_records} new records.")
        record_ingestion_metadata(engine, file_hash, total_new_records, max_complaint_id, remote_last_modified)

        logging.info("\nIngestion Summary:")
        logging.info(f"- New records ingested: {total_new_records}")

    except Exception as e:
        logging.error(f"Error: {str(e)}", exc_info=True)
        log_db(engine, "Ingestion Pipeline", "ERROR", f"Ingestion failed: {e}")
        raise