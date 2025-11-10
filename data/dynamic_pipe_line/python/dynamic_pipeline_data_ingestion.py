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
            with zip_ref.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)

        if limit is not None:
            logging.warning(f"Limiting ingestion to the first {limit} records for this run.")
            df = df.head(limit)
            log_db(engine, "Record Limiting", "INFO", f"Using only the first {limit} records.")

        logging.info(f"Extracted {len(df)} records from {csv_filename}.")
        log_db(engine, "Extract CSV", "SUCCESS", f"Extracted {len(df)} records from {csv_filename}")

        # --- Sanitize all column names first ---
        df.columns = [col.lower().replace(' ', '_').replace('?', '').replace('-', '_') for col in df.columns]

        df["ingestion_date"] = ingestion_date
        df["source_file_name"] = source_file_name
        df["complaint_id"] = pd.to_numeric(df["complaint_id"], errors="coerce")

        logging.info("Checking for existing complaint IDs...")
        with engine.connect() as conn:
            # Ensure the table exists before querying
            if engine.dialect.has_table(conn, "consumer_complaints_raw"):
                existing_ids = pd.read_sql("SELECT complaint_id FROM consumer_complaints_raw", conn)
                existing_ids_set = set(existing_ids["complaint_id"].dropna().astype(int))
            else:
                existing_ids_set = set()

        df_new = df[~df["complaint_id"].isin(existing_ids_set)]
        logging.info(f"Found {len(df_new)} new records to ingest.")
        log_db(engine, "Deduplication", "SUCCESS", f"{len(df_new)} new records identified")

        start_ingest = time.time()
        chunk_size = 10000  # Increased for better performance

        if not df_new.empty:
            logging.info("Ingesting new records...")
            for i in tqdm(range(0, len(df_new), chunk_size), desc="Ingesting to MySQL"):
                df_new.iloc[i:i+chunk_size].to_sql("consumer_complaints_raw", con=engine, if_exists="append", index=False)
            end_ingest = time.time()
            ingest_time = round(end_ingest - start_ingest, 2)
            logging.info(f"Ingested {len(df_new)} records in {ingest_time} seconds.")
            log_db(engine, "Ingest to MySQL", "SUCCESS", f"Ingested {len(df_new)} records", duration=ingest_time)
        else:
            logging.info("No new records to ingest.")
            log_db(engine, "Ingest to MySQL", "SUCCESS", "No new records to ingest")

        max_id = df["complaint_id"].dropna().astype(int).max()
        record_ingestion_metadata(engine, file_hash, len(df_new), max_id, remote_last_modified)

        logging.info("\nIngestion Summary:")
        logging.info(f"- Total records in file: {len(df)}")
        logging.info(f"- New records ingested: {len(df_new)}")
        if not df_new.empty:
            logging.info(f"- Ingestion time: {ingest_time} seconds")

    except Exception as e:
        logging.error(f"Error: {str(e)}", exc_info=True)
        log_db(engine, "Ingestion Pipeline", "ERROR", f"Ingestion failed: {e}")
        raise