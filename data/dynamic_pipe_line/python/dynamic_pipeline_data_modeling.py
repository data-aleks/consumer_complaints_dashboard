import logging
from dotenv import load_dotenv
import os

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join("python", ".db_config.env"))

# SQL script paths for data modeling
modeling_scripts = [
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_company.sql", "Create company dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_consent.sql", "Create consent dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_date.sql", "Create date dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_disputed.sql", "Create disputed dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_issue.sql", "Create issue dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_origin.sql", "Create origin dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_product.sql", "Create product dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_public_response.sql", "Create public_response dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_state.sql", "Create state dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_status.sql", "Create status dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_sub_issue.sql", "Create sub_issue dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_sub_product.sql", "Create sub_product dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_tag.sql", "Create tag dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_dim_zip_code.sql", "Create zip_code dimension"),
    ("python/sql/data_modeling/cfpb_consumer_complaints_fact_complaint.sql", "Create fact_complaint table"),
]

# === LOGGING SETUP ===
log_file = "cfpb_data_modeling_log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def log_action(action, status, message):
    if status.lower() == "success":
        logging.info(f"{action} | SUCCESS | {message}")
    else:
        logging.error(f"{action} | ERROR | {message}")

# === MAIN RUN FUNCTION ===
def run(engine, sql_executor):
    logging.info("🔗 Connecting to MySQL...")
    logging.info("🏗️ Starting data modeling pipeline...")
    for script_path, label in modeling_scripts:
        sql_executor(script_path, label)
    logging.info("✅ All data modeling steps completed.")