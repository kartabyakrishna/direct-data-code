# Veeva Direct Data to Redshift Connector - Functional Specification

This document details the step-by-step functionality of the Veeva Direct Data to Redshift pipeline. It describes the end-to-end process from initiating the job to loading data into Redshift, including mechanisms for state management, data transformation, and schema evolution.

## 1. System Overview

The solution is an **incremental ELT (Extract, Load, Transform) pipeline** designed to synchronize Veeva Vault Direct Data files into an Amazon Redshift data warehouse.

-   **Runtime Environment**: AWS Glue (Python Shell w/ 1 DPU).
-   **Source**: Veeva Vault (Direct Data API).
-   **Staging**: AWS S3 (Raw archives and processed Parquet/CSV files).
-   **Destination**: Amazon Redshift.
-   **State Management**: AWS DynamoDB (Watermarking).
-   **Scheduling**:
    1.  **Incremental Job**: Runs every **15 minutes** to process `Incremental` extracts.
    2.  **Log Job**: Runs **Daily** (e.g., at 00:00 UTC) to process `System Log` extracts for audit trails.

## 2. Step-by-Step Execution Flow

### Step 1: Initialization & Configuration
**File**: `pipeline_main.py`
1.  **Load Configuration**:
    -   Reads connector configuration from `connector_config.json` (S3/Redshift settings).
    -   Reads VAPIL settings from `vapil_settings.json` (Vault credentials).
    -   Accepts environment variable overrides (e.g., `process_id`, `state_table_name`, `extract_type`).
2.  **Initialize Services**:
    -   `AwsS3Service`: For all object storage operations.
    -   `RedshiftService`: For database connections and query execution.
    -   `VaultService`: For Direct Data API interactions.
    -   `DynamoDBStateManager`: For tracking incremental progress.
    -   `FailureHandler`: For sending SNS alerts handling operational failures.

### Step 2: State Management (Watermark Retrieval)
**File**: `pipeline_main.py`, `common/services/state_manager.py`
1.  The system queries DynamoDB (`VeevaStateTable`) using the `process_id` (default: `VeevaRedshift_Incremental`) to retrieve the `last_processed_time` (watermark).
2.  **Incremental Logic**:
    -   If a watermark exists, the `start_time` for the Direct Data request is set to this timestamp.
    -   If no watermark exists (first run), it defaults to the configuration's start time or a dynamic lookback window (e.g., last 24 hours) if `USE_DYNAMIC_WINDOW=true`.

### Step 3: Vault Extraction (File Discovery & Download)
**File**: `common/scripts/direct_data_to_object_storage.py`
1.  **API Request**: Calls `Retrieve Available Direct Data Files` endpoint on Vault with the `extract_type` (e.g., `incremental`) and the calculated time window.
2.  **Filtering**:
    -   Ignores files with 0 records.
    -   Filters out files that are older than or equal to the current watermark (idempotency check).
3.  **Downloading**:
    -   Downloads *every* valid Direct Data file (`.tar.gz`) from Vault.
    -   **Multi-part Uploads**: If a file is large (multiple parts), it streams chunks directly to S3 using multi-part upload to minimize memory usage.
    -   **Artifact**: Files are saved to `s3://<bucket>/<extract_type>/<filename>.tar.gz`.

### Step 4: Transactional Processing Loop (Sequential)
**File**: `pipeline_main.py`
The system processes downloaded files **SEQUENTIALLY** (sorted by timestamp) to ensure data consistency and integrity.
> **Note**: Parallel processing is intentionally avoided at the file level to prevent race conditions during schema updates and to guarantee that incremental changes (Create -> Update -> Delete) are applied in the exact order they occurred in Vault.

#### A. Staging & Transformation
**File**: `common/scripts/download_and_unzip_direct_data_files.py`
1.  **Download**: Fetches the specific `.tar.gz` from S3 to the local Glue environment.
2.  **Metadata Extraction**: First pass scans for `metadata.csv` or `metadata_full.csv` to build an in-memory schema registry.
3.  **Extraction & Conversion**:
    -   Iterates through every file in the TAR archive.
    -   **Parquet Conversion (Optional but Recommended)**:
        -   If `convert_to_parquet` is True, reads CSVs in chunks (100k rows).
        -   **Type Mapping**: detailed mapping of Vault types (Picklist, DateTime, Boolean, etc.) to PyArrow types using the metadata.
        -   **Decimal Handling**: Auto-detects if "Number" fields contain decimals to convert to `float64` vs `int64`.
        -   Writes converted `.parquet` files.
    -   **Upload**: Uploads the extracted (and optionally converted) files back to S3 in a subfolder structure.

#### B. Redshift Schema Evolution
**File**: `common/scripts/load_data.py`
Before loading data, the system adapts the Redshift schema to match the incoming data:
1.  **Manifest Analysis**: Reads the `manifest` file from the extracted data. identifies `updates` and `deletes`.
2.  **Handle Deletes**:
    -   If a table or column is marked for deletion in the manifest, the system issues `DROP TABLE` or `ALTER TABLE DROP COLUMN` commands.
3.  **Handle Updates (Schema Drift)**:
    -   Compares incoming `metadata` columns with existing Redshift table columns (`information_schema` check).
    -   **Add Columns**: Issues `ALTER TABLE ADD COLUMN` for new fields.
    -   **Modify Columns**: Issues `ALTER TABLE ...` to change data types if compatible.

#### C. Data Loading (Upsert)
**File**: `common/services/database_service.py`
The loading process ensures 0 duplicates and handles updates correctly:
1.  **Pre-Load Cleanup (Incremental Only)**:
    -   Uses the `manifest` to identify IDs of records that have changed or been deleted.
    -   Executes a `DELETE FROM <table> WHERE id IN (...)` to remove old versions of these records from Redshift.
2.  **COPY Command**:
    -   Constructs a Redshift `COPY` command.
    -   Loads data from the S3 path (Parquet or CSV) into the target table.
    -   Uses IAM Role authentication for the COPY operation.

### Step 5: Commit & State Update
**File**: `pipeline_main.py`
1.  **Commit**: If all load operations for the file succeed, the Redshift transaction is committed (`COMMIT`).
2.  **Update Watermark**: The `processed_timestamp` of the file is written to DynamoDB. This ensures that if the job fails *after* this point, this file is not re-processed.

### Step 6: Failure Handling
**File**: `pipeline_main.py`
1.  **Rollback**: If *any* step (Unzip, Schema Change, load) fails for a specific file:
    -   The Redshift transaction is rolled back (`ROLLBACK`).
    -   The watermark is *not* updated.
    -   The job stops immediately.
2.  **Alerting**:
    -   The `FailureHandler` publishes a message to the SNS Topic (`Veeva-Pipeline-Alerts`).
    -   (Optional) Can trigger EventBridge rule disabling to prevent future runs until fixed.

## 3. Key Features
-   **Dual Scheduling**: Separate triggers for frequent Incremental updates (15m) and comprehensive Daily Logs ensure audit compliance without impacting near-real-time performance.
-   **Sequential Concurrency Model**: Strict sequential verification prevents "out-of-order" updates (e.g., processing a Delete before an Update for the same record), ensuring the Redshift state always mirrors Vault perfectly.
-   **Idempotency**: Leveraging DynamoDB watermarks ensures the pipeline can restart safely after failure without duplicating data.
-   **Schema Evolution**: Automatically adapts Redshift tables to changes in Veeva Vault (new fields, new objects).
-   **Memory Efficiency**: Uses chunked processing (Pandas/PyArrow) to handle large datasets within limited Glue memory (1 DPU).
-   **Type Safety**: Explicit type conversion during Parquet generation prevents common CSV load errors in Redshift.
