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
import os

# === CONFIG ===
source_url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
source_file_name = "complaints.csv"
ingestion_date = datetime.today().date()

# Load DB credentials from .env
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# === LOGGING SETUP ===
log_file = "cfpb_ingestion_log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def log_action(action, status, message):
    if status.lower() == "success":
        logging.info(f"{action} | SUCCESS | {message}")
    else:
        logging.error(f"{action} | ERROR | {message}")

def compute_file_hash(bytes_obj):
    hasher = hashlib.sha256()
    hasher.update(bytes_obj.getvalue())
    return hasher.hexdigest()

def get_last_ingested_hash(engine):
    query = "SELECT file_hash FROM ingestion_metadata ORDER BY ingested_at DESC LIMIT 1"
    with engine.connect() as conn:
        return conn.execute(text(query)).scalar()

def record_ingestion_metadata(engine, file_hash, row_count, max_id):
    insert_sql = text("""
        INSERT INTO ingestion_metadata (source_file_name, file_hash, row_count, max_complaint_id)
        VALUES (:filename, :file_hash, :row_count, :max_id)
    """)
    with engine.connect() as conn:
        conn.execute(insert_sql, {
            "filename": source_file_name,
            "file_hash": file_hash,
            "row_count": row_count,
            "max_id": max_id
        })

# === MAIN RUN FUNCTION ===
def run(engine):
    try:
        print("📥 Starting download...")
        start_download = time.time()
        response = requests.get(source_url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        zip_bytes = io.BytesIO()

        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading ZIP") as pbar:
            for data in response.iter_content(block_size):
                zip_bytes.write(data)
                pbar.update(len(data))

        file_hash = compute_file_hash(zip_bytes)
        last_hash = get_last_ingested_hash(engine)
        if file_hash == last_hash:
            print("🛑 No changes detected in source file. Skipping ingestion.")
            log_action("Ingestion Skipped", "success", "File hash unchanged. No ingestion needed.")
            return

        end_download = time.time()
        download_time = round(end_download - start_download, 2)
        downloaded_mb = zip_bytes.tell() / 1024 / 1024
        print(f"✅ Downloaded {downloaded_mb:.2f} MB in {download_time} seconds.")
        log_action("Download ZIP", "success", f"Downloaded {downloaded_mb:.2f} MB in {download_time} seconds")

        print("📦 Extracting CSV...")
        with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
            csv_filename = [f for f in zip_ref.namelist() if f.endswith('.csv')][0]
            with zip_ref.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)

        print(f"✅ Extracted {len(df)} records from {csv_filename}.")
        log_action("Extract CSV", "success", f"Extracted {len(df)} records from {csv_filename}")

        df["ingestion_date"] = ingestion_date
        df["source_file_name"] = source_file_name
        df["complaint_id"] = pd.to_numeric(df["Complaint ID"], errors="coerce")

        print("🧹 Checking for existing complaint IDs...")
        with engine.connect() as conn:
            existing_ids = pd.read_sql("SELECT `Complaint ID` FROM consumer_complaints_raw", conn)
            existing_ids_set = set(existing_ids["Complaint ID"].dropna().astype(int))

        df_new = df[~df["Complaint ID"].isin(existing_ids_set)]
        print(f"✅ Found {len(df_new)} new records to ingest.")
        log_action("Deduplication", "success", f"{len(df_new)} new records identified")

        start_ingest = time.time()
        chunk_size = 5000
        df_new = df_new.drop(columns=["complaint_id"])

        if not df_new.empty:
            print("📤 Ingesting new records...")
            for i in tqdm(range(0, len(df_new), chunk_size), desc="Ingesting to MySQL"):
                df_new.iloc[i:i+chunk_size].to_sql("consumer_complaints_raw", con=engine, if_exists="append", index=False)
            end_ingest = time.time()
            ingest_time = round(end_ingest - start_ingest, 2)
            print(f"✅ Ingested {len(df_new)} records in {ingest_time} seconds.")
            log_action("Ingest to MySQL", "success", f"Ingested {len(df_new)} records in {ingest_time} seconds")
        else:
            print("✅ No new records to ingest.")
            log_action("Ingest to MySQL", "success", "No new records to ingest")

        max_id = df["Complaint ID"].dropna().astype(int).max()
        record_ingestion_metadata(engine, file_hash, len(df_new), max_id)

        print("\n📊 Ingestion Summary:")
        print(f"- File downloaded: {downloaded_mb:.2f} MB")
        print(f"- Download time: {download_time} seconds")
        print(f"- Total records in file: {len(df)}")
        print(f"- New records ingested: {len(df_new)}")
        if not df_new.empty:
            print(f"- Ingestion time: {ingest_time} seconds")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        log_action("Pipeline Execution", "error", str(e))