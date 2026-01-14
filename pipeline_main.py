import sys
import os
import boto3
import traceback
import json
from datetime import datetime, timedelta

# Ensure local modules are found
sys.path.append(os.path.join(os.path.dirname(__file__), 'veeva_accelerator'))

from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService
from common.services.state_manager import DynamoDBStateManager
from common.services.failure_handler import FailureHandler
from common.utilities import read_json_file, log_message
from accelerators.redshift.services.redshift_service import RedshiftService
from common.scripts import direct_data_to_object_storage, download_and_unzip_direct_data_files, load_data, extract_doc_content, retrieve_doc_text

def main():
    # --- Configuration ---
    # Parse CLI Arguments for Runtime Overrides (Critical for EventBridge Triggers)
    # Glue passes arguments as --KEY VALUE. We need to parse these to override defaults.
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--DIRECT_DATA_EXTRACT_TYPE", help="Override extract type (e.g., 'full', 'log', 'incremental')", default=os.environ.get("DIRECT_DATA_EXTRACT_TYPE"))
    parser.add_argument("--DIRECT_DATA_START_TIME", help="Override start time", default=os.environ.get("DIRECT_DATA_START_TIME"))
    parser.add_argument("--DIRECT_DATA_STOP_TIME", help="Override stop time", default=os.environ.get("DIRECT_DATA_STOP_TIME"))
    parser.add_argument("--EVENT_RULE_NAME", help="Event Rule to disable on failure", default=os.environ.get("EVENT_RULE_NAME"))
    parser.add_argument("--RECOVERY_MODE", help="Auto-enable scheduler on success", action='store_true')
    
    # Use parse_known_args because Glue passes many internal arguments we don't care about
    args, _ = parser.parse_known_args()

    config_filepath = os.environ.get("CONNECTOR_CONFIG_PATH", "/app/config/connector_config.json")
    vapil_settings_filepath = os.environ.get("VAPIL_SETTINGS_PATH", "/app/config/vapil_settings.json")
    
    # Infrastructure Config
    state_table_name = os.environ.get("DYNAMODB_STATE_TABLE", "VeevaStateTable")
    process_id = os.environ.get("PROCESS_ID", "VeevaRedshift_Incremental")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    event_rule_name = args.EVENT_RULE_NAME
    
    # Initialize Failure Handler immediately
    failure_handler = FailureHandler(sns_topic_arn, event_rule_name)

    def get_secret(secret_name):
        client = boto3.client('secretsmanager')
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            if 'SecretString' in get_secret_value_response:
                return json.loads(get_secret_value_response['SecretString'])
        except Exception as e:
            print(f"Error retrieving secret {secret_name}: {e}")
            raise e
            
    try:
        # Load Config from Secrets Manager
        # We expect a single JSON with keys: "connector_config" and "vapil_settings"
        # OR we merge them. The user mentioned "that 2 jsons".
        # Let's assume the Secret contains the MERGED content or keys for each.
        # Given the previous files:
        # connector_config has: direct_data, s3, redshift keys.
        # vapil_settings has: authenticationType, etc.
        # Strategy: Fetch secret, if it has 'connector_config' key use that, else assume it IS the config.
        # But we have 2 distinct usages: config_params (for S3/Redshift) and vault_service (init with file path).
        # VaultService expects a file path usually? Let's check init.
        # If VaultService takes a dict or path, we need to handle that.
        
        # Checking VaultService usage:
        # vault_service = VaultService(vapil_settings_filepath)
        # We need to see if VaultService can accept a DICT. If not, we must write the secret to a temp file.
        
        secret_name = os.environ.get("VEEVA_CONFIG_SECRET", "VeevaRedshiftConfig-dev")
        full_config = get_secret(secret_name)
        
        config_params = full_config.get('connector_config', full_config) # Fallback if structure differs
        vapil_settings = full_config.get('vapil_settings', {})
        
        direct_data_params = config_params['direct_data']
        
        # Overrides (Extract Type) - Priority: CLI Args > Env Var > Config File
        if args.DIRECT_DATA_EXTRACT_TYPE:
            print(f"Overriding Extract Type to: {args.DIRECT_DATA_EXTRACT_TYPE}")
            direct_data_params['extract_type'] = args.DIRECT_DATA_EXTRACT_TYPE

        # Initialize Services
        s3_service = AwsS3Service(config_params['s3'])
        s3_service.convert_to_parquet = config_params.get('convert_to_parquet', False)
        
        redshift_params = config_params['redshift']
        redshift_params['convert_to_parquet'] = config_params['convert_to_parquet']
        redshift_params['object_storage_root'] = f's3://{config_params["s3"]["bucket_name"]}'
        redshift_service = RedshiftService(redshift_params)
        
        # VaultService Config Injection
        # We write vapil_settings to a temp file because likely legacy VaultService reads from disk
        import tempfile
        import json
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            json.dump(vapil_settings, tmp)
            tmp_vapil_path = tmp.name
            
        vault_service = VaultService(tmp_vapil_path)
        state_manager = DynamoDBStateManager(table_name=state_table_name)

        # --- State Management (Watermark vs Manual Override) ---
        last_watermark = None
        
        # Priority 1: Manual CLI Override (e.g., for backfill or specific interval)
        if args.DIRECT_DATA_START_TIME:
            print(f"--- [MANUAL] Using Manual Start Time: {args.DIRECT_DATA_START_TIME} ---")
            direct_data_params['start_time'] = args.DIRECT_DATA_START_TIME
            
            # If manual start is provided, we also check for manual stop (optional)
            if args.DIRECT_DATA_STOP_TIME:
                 print(f"--- [MANUAL] Using Manual Stop Time: {args.DIRECT_DATA_STOP_TIME} ---")
                 direct_data_params['stop_time'] = args.DIRECT_DATA_STOP_TIME

        # Priority 2: Watermark (Standard Incremental Logic)
        else:
            last_watermark = state_manager.get_last_processed_time(process_id)
            if last_watermark:
                print(f"Resuming from Watermark: {last_watermark}")
                direct_data_params['start_time'] = last_watermark
            else:
                 print("No Watermark found. Using default start_time from config.")

        # Force Dynamic Window Calculation if no watermark and USE_DYNAMIC_WINDOW is set
        # This handles the "Day 1" or "Lost Watermark" fallback logic if needed

        # --- 1. Identify Files to Process ---
        print("--- Step 1: Retrieving File List from Veeva ---")
        # direct_data_to_object_storage.run now downloads files and returns a list
        # We need to act carefully here. Ideally we want to just LIST first, then process.
        # But the current script logic DOWNLOADS and returns metadata. 
        # Refactoring `direct_data_to_object_storage` to just LIST would be cleaner, but for now:
        # It downloads ALL files in the window. We then loop through them.
        
        processed_files = direct_data_to_object_storage.run(vault_service=vault_service,
                                                          object_storage_service=s3_service,
                                                          direct_data_params=direct_data_params) or []


        # sort sequentially
        processed_files.sort(key=lambda x: x['timestamp'])

        files_to_process = []
        for f in processed_files:
            # Idempotency Check: if timestamp <= last_watermark, skip
            if last_watermark and f['timestamp'] <= last_watermark:
                print(f"Skipping already processed file: {f['filename']} (Time: {f['timestamp']})")
                continue
            files_to_process.append(f)

        if not files_to_process:
            print("No new files to process.")
            # Even if no files, if we are in Recovery Mode, we should probably re-enable 
            # (assuming 'no files' means we are up to date)
            if args.RECOVERY_MODE:
                print("--- [RECOVERY] System is up to date. Re-enabling Scheduler. ---")
                failure_handler.enable_scheduler()
                failure_handler.publish_alert(
                    error_message="Recovery Run Complete (No new files).",
                    context={"status": "Scheduler Re-enabled"}
                )
            return

        # --- 2. Process Loop (Transactional) ---
        print(f"--- Processing {len(files_to_process)} new files ---")
        
        for file_info in files_to_process:
            filename = file_info['filename']
            print(f"--- [START] Processing File: {filename} ---")
            
            try:
                # A. Begin Transaction
                redshift_service.db_connection.begin_transaction()
                
                # B. Prepare S3 Service (point to specific archive)
                s3_service.archive_filepath = file_info['object_path']
                
                # C. Unzip & Prepare
                download_and_unzip_direct_data_files.run(object_storage_service=s3_service)
                
                # D. Load to Redshift
                load_data.run(object_storage_service=s3_service,
                              database_service=redshift_service,
                              direct_data_params=direct_data_params,
                              redshift_params=redshift_params)
                              
                # E. Commit Transaction
                redshift_service.db_connection.commit_transaction()
                print(f"--- [COMMIT] Redshift Transaction Committed for {filename} ---")
                
                # F. Update State (Watermark)
                state_manager.set_last_processed_time(process_id, file_info['timestamp'])
                print(f"--- [STATE] Watermark updated to {file_info['timestamp']} ---")
                
            except Exception as e:
                print(f"!!! [FAILURE] Error processing file {filename} !!!")
                # G. Rollback
                redshift_service.db_connection.rollback_transaction()
                print("--- [ROLLBACK] Redshift Transaction Rolled Back ---")
                
                # Rais to Main Loop Handler
                raise e
        
        # --- 3. RECOVERY MODE: Auto-Enable Scheduler ---
        if args.RECOVERY_MODE:
            print("--- [RECOVERY] All files processed successfully. Re-enabling Scheduler. ---")
            failure_handler.enable_scheduler()
            failure_handler.publish_alert(
                error_message="Recovery Run Complete. Data Synced.",
                context={
                    "status": "Scheduler Re-enabled",
                    "files_processed": len(files_to_process),
                    "last_processed_time": files_to_process[-1]['timestamp']
                }
            )

    except Exception as e:
        log_message('Error', "Critical Pipeline Failure", exception=e)
        # H. Critical Failure Handler (SNS + Disable Scheduler)
        failure_handler.handle_critical_failure(e, context={"last_watermark": last_watermark if 'last_watermark' in locals() else "Unknown"})
        sys.exit(1)

if __name__ == "__main__":
    main()
