# Dynamic ETL Pipeline for CFPB Complaint Data

This project implements a robust, dynamic, and incremental ETL (Extract, Transform, Load) pipeline for processing consumer complaint data from the Consumer Financial Protection Bureau (CFPB). The pipeline is designed to be modular, resilient, and efficient, making it suitable for recurring data processing tasks.

## Features

- **Dynamic & Incremental Processing**: Only new or unprocessed records are handled in each run, thanks to timestamping and metadata tracking.
- **Modular Architecture**: The pipeline is broken down into distinct, runnable steps: Ingestion, Cleaning, Insertion, and Modeling.
- **Database-Driven Logging**: All major pipeline events, successes, and failures are logged to a `pipeline_logs` table for easy monitoring and auditing.
- **Configuration Driven**: Database credentials and settings are managed via a `.db_config.env` file, keeping sensitive information out of the code.
- **Automated Schema Setup**: The pipeline automatically creates necessary tables and indexes on its first run.
- **Data Modeling**: Transforms the cleaned, flat data into a star schema with fact and dimension tables, ready for BI and analytics.
- **Command-Line Interface**: Easily control which part of the pipeline to run (`ingest`, `clean`, `model`, etc.) and limit the number of records for development and testing.

---

## Project Structure

```
dynamic_pipe_line/
├── python/
│   ├── run_pipeline.py               # Main entry point and orchestrator
│   ├── dynamic_pipeline_data_ingestion.py  # Step 1: Data download and raw ingestion
│   ├── dynamic_pipeline_data_clean.py      # Step 2: In-place data cleaning
│   ├── dynamic_pipeline_data_insert.py     # Step 3: Insert cleaned data into a new table
│   ├── dynamic_pipeline_data_modeling.py   # Step 4: Build star schema (facts /dimensions)
│   ├── pipeline_utils.py             # Shared utility functions (e.g., SQL executor)
│   ├── pipeline_logger.py            # Utility for logging to the database
│   ├── .db_config.env                # Database configuration (MUST BE CREATED)
│   └── sql/
│       ├── setup/                    # SQL for initial table creation
│       ├── data_cleaning/            # SQL scripts for various cleaning tasks
│       ├── data_insertion/           # SQL for inserting data and schema migrations
│       └── data_modeling/            # SQL for creating dimension and fact tables
├── pipeline_run.log                  # Main log file for pipeline execution
└── README.md                         # This file
```

---

## Prerequisites

- Python 3.8+
- MySQL Server (or another compatible SQL database)
- Git

## Setup and Installation

1.  **Clone the Repository**
    ```bash
    git clone <your-repository-url>
    cd dynamic_pipe_line
    ```

2.  **Create a Virtual Environment & Install Dependencies**
    It is highly recommended to use a virtual environment to manage project-specific dependencies.

    *   **On macOS/Linux:**
        ```bash
        python3 -m venv venv
        source venv/bin/activate
        ```

    *   **On Windows:**
        ```bash
        python -m venv venv
        .\venv\Scripts\activate
        ```

    *   **Install the required packages:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up the Database**
    - Ensure your MySQL server is running.
    - Create a database for this project, for example:
      ```sql
      CREATE DATABASE cfpb_complaints;
      ```

4.  **Configure Database Connection**
    - In the `python/` directory, create a file named `.db_config.env`.
    - Add your database credentials to this file. It should look like this:
      ```env
      DB_USER=your_username
      DB_PASSWORD="your_secret_password"
      DB_HOST=localhost
      DB_PORT=3306
      DB_NAME=database_name
      ```

---

## ⚠️ Important Note: Data Size and Performance

Please be aware that this pipeline processes a very large dataset.

-   **File Download**: The source `.zip` file is approximately **1.6 GB**.
-   **Database Size**: When fully ingested and processed, the data will occupy around **8 GB** in your MySQL database.
-   **Row Count**: The dataset contains over **12 million rows**.

### Initial Processing Time

The first end-to-end run of the entire dataset is time-consuming. For reference, on a test system with the following specifications, the initial load (ingestion, cleaning, insertion, and modeling) took approximately **1 to 1.5 hours**, not including the initial file download time.

-   **Test System**: Asus Vivobook S16
-   **CPU**: Ryzen 9 AI
-   **RAM**: 32 GB
-   **Database**: Local MySQL instance

### Recommendation for Testing

It is **strongly recommended** to use the `--limit` parameter for development, testing, or your first few runs to ensure everything is configured correctly without waiting for the full process.

---

## How to Run the Pipeline

The pipeline is executed from the `python/` directory using `run_pipeline.py`.

```bash
cd python
```

### Available Commands

*   **Run the entire pipeline from start to finish:**
    ```bash
    python run_pipeline.py --step all
    ```

*   **Run a specific step:**
    The `--step` argument allows you to run a single part of the process.
    ```bash
    # Run only the data ingestion step
    python run_pipeline.py --step ingest

    # Run only the data cleaning step
    python run_pipeline.py --step clean

    # Run only the data insertion step
    python run_pipeline.py --step insert

    # Run only the data modeling step
    python run_pipeline.py --step model
    ```

*   **Limit the number of records (for development/testing):**
    The `--limit` argument is useful for quick test runs on a subset of data.
    ```bash
    # Ingest and process only 10,000 records
    python run_pipeline.py --step all --limit 10000
    ```

---

## Pipeline Architecture

The pipeline is designed as a sequence of idempotent steps.

### 1. Data Ingestion (`ingest`)
- **Downloads**: Fetches the `complaints.csv.zip` file from the CFPB website.
- **Update Check**: It first checks the remote file's `Last-Modified` header and compares it against metadata from the last successful run. It also uses a local file hash to avoid re-processing the exact same file.
- **Extraction**: Unzips the file and loads `complaints.csv` into a pandas DataFrame.
- **Deduplication**: Checks for existing `complaint_id`s in the `consumer_complaints_raw` table to ensure only new records are ingested.
- **Loading**: Ingests the new, raw records into the `consumer_complaints_raw` table.
- **Metadata Logging**: Records the file hash, number of new rows, and server modification date into the `ingestion_metadata` table.

### 2. Data Cleaning (`clean`)
- **Identifies Records**: Selects records from `consumer_complaints_raw` where `cleaned_timestamp` is `NULL`.
- **Executes SQL Scripts**: Runs a series of SQL scripts from `sql/data_cleaning/` to perform in-place cleaning and standardization (e.g., formatting dates, standardizing state codes, handling NULLs).
- **Timestamping**: Once cleaning is complete, it updates the `cleaned_timestamp` for the processed rows, ensuring they are not cleaned again in the next run.

### 3. Data Insertion (`insert`)
- **Identifies Records**: Selects records from `consumer_complaints_raw` that have been cleaned (`cleaned_timestamp` is NOT NULL) but have not yet been inserted into the final cleaned table.
- **Loading**: Inserts these cleaned records into the `consumer_complaints_cleaned` table. This separation keeps the raw data immutable and provides a clear, clean source for the modeling step.

### 4. Data Modeling (`model`)
- **Identifies Records**: Selects records from `consumer_complaints_cleaned` where `modeling_timestamp` is `NULL`.
- **Populates Dimensions**: Runs SQL scripts to populate dimension tables (`dim_product`, `dim_company`, etc.) with distinct values from the new data. `INSERT IGNORE` is used to avoid duplicates.
- **Populates Fact Table**: Joins the `consumer_complaints_cleaned` table with the newly populated dimension tables to create entries in the `fact_complaints` table.
- **Timestamping**: Updates the `modeling_timestamp` in the `consumer_complaints_cleaned` table for the processed rows.

## Database Schema

- **`consumer_complaints_raw`**: Stores the raw, unaltered data exactly as it was ingested from the source file, with added metadata columns like `ingestion_date` and `cleaned_timestamp`.
- **`consumer_complaints_cleaned`**: Stores the data after it has passed through the cleaning and insertion steps. This table is the clean source for all downstream analytics and modeling. It includes a `modeling_timestamp` column.
- **`dim_*` Tables**: A series of dimension tables (e.g., `dim_date`, `dim_product`, `dim_company`) that store unique values for categorical data, forming a star schema.
- **`fact_complaints`**: The central fact table of the star schema, containing foreign keys to all dimension tables and the core numeric/narrative data of each complaint.
- **`ingestion_metadata`**: Tracks each ingestion event, including file hash and row counts, to prevent duplicate processing.
- **`pipeline_logs`**: A comprehensive log of all pipeline steps, their status (SUCCESS/ERROR), duration, and any relevant messages.