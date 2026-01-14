import sys
import os
import argparse
import boto3
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService
from common.services.state_manager import DynamoDBStateManager
from common.services.failure_handler import FailureHandler
from common.utilities import read_json_file, log_message
from accelerators.redshift.services.redshift_service import RedshiftService
from common.scripts import direct_data_to_object_storage, download_and_unzip_direct_data_files, load_data

# Ensure local modules are found
sys.path.append(os.path.join(os.path.dirname(__file__), 'veeva_accelerator'))

def run_recovery(start_time_override=None):
    # --- Configuration ---
    config_filepath = os.environ.get("CONNECTOR_CONFIG_PATH", "/app/config/connector_config.json")
    vapil_settings_filepath = os.environ.get("VAPIL_SETTINGS_PATH", "/app/config/vapil_settings.json")
    
    state_table_name = os.environ.get("DYNAMODB_STATE_TABLE", "VeevaStateTable")
    process_id = os.environ.get("PROCESS_ID", "VeevaRedshift_Incremental")
    event_rule_name = os.environ.get("EVENT_RULE_NAME")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN") # needed for generic init

    failure_handler = FailureHandler(sns_topic_arn, event_rule_name)

    print(f"--- STARTING RECOVERY MODE ---")
    print(f"Goal: Process pending files and RE-ENABLE Scheduler: {event_rule_name}")

    try:
        config_params = read_json_file(config_filepath)
        direct_data_params = config_params['direct_data']
        
        # Initialize Services
        s3_service = AwsS3Service(config_params['s3'])
        redshift_params = config_params['redshift']
        redshift_params['object_storage_root'] = f's3://{config_params["s3"]["bucket_name"]}'
        redshift_service = RedshiftService(redshift_params)
        vault_service = VaultService(vapil_settings_filepath)
        state_manager = DynamoDBStateManager(table_name=state_table_name)

        # Determine Start Time
        if start_time_override:
            print(f"Using Override Start Time: {start_time_override}")
            direct_data_params['start_time'] = start_time_override
        else:
            last_watermark = state_manager.get_last_processed_time(process_id)
            if last_watermark:
                print(f"Resuming from Watermark: {last_watermark}")
                direct_data_params['start_time'] = last_watermark
            else:
                print("No watermark found. Please provide --start-time argument.")
                sys.exit(1)

        # Get Files
        processed_files = direct_data_to_object_storage.run(vault_service=vault_service,
                                                          object_storage_service=s3_service,
                                                          direct_data_params=direct_data_params) or []
        processed_files.sort(key=lambda x: x['timestamp'])

        if not processed_files:
            print("No files to process. Scheduler will differ.")
        
        # Process Loop
        for file_info in processed_files:
            print(f"--- [RECOVERY] Processing File: {file_info['filename']} ---")
            try:
                redshift_service.db_connection.begin_transaction()
                s3_service.archive_filepath = file_info['object_path']
                download_and_unzip_direct_data_files.run(object_storage_service=s3_service)
                load_data.run(object_storage_service=s3_service,
                              database_service=redshift_service,
                              direct_data_params=direct_data_params)
                redshift_service.db_connection.commit_transaction()
                state_manager.set_last_processed_time(process_id, file_info['timestamp'])
                print(f"--- [RECOVERY] Success for {file_info['filename']} ---")
            except Exception as e:
                redshift_service.db_connection.rollback_transaction()
                print(f"!!! [RECOVERY FAILED] Error processing file {file_info['filename']} !!!")
                raise e

        # On Full Success: Re-enable Scheduler
        print("--- All files processed successfully. Re-enabling Scheduler... ---")
        failure_handler.enable_scheduler()
        print("--- RECOVERY COMPLETE. Operations Normalized. ---")

    except Exception as e:
        print(f"Recovery Script Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Manual Recovery Script for Veeva Connector')
    parser.add_argument('--start-time', help='Override start time (ISO 8601)', required=False)
    args = parser.parse_args()
    
    run_recovery(args.start_time)
