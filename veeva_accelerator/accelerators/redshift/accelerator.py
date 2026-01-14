import sys
import os
from datetime import datetime, timedelta
from common.services.state_manager import StateManager



from accelerators.redshift.services.redshift_service import RedshiftService

sys.path.append('.')
from common.scripts import (direct_data_to_object_storage, download_and_unzip_direct_data_files,
                            extract_doc_content, load_data, retrieve_doc_text)
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService
from common.utilities import read_json_file


def main():
    config_filepath: str = os.environ.get("CONNECTOR_CONFIG_PATH", "path/to/connector_config.json")
    vapil_settings_filepath: str = os.environ.get("VAPIL_SETTINGS_PATH", "path/to/vapil_settings.json")


    config_params: dict = read_json_file(config_filepath)
    direct_data_params: dict = config_params['direct_data']
    
    # Dynamic Configuration Overrides
    env_extract_type = os.environ.get("DIRECT_DATA_EXTRACT_TYPE")
    if env_extract_type:
        direct_data_params['extract_type'] = env_extract_type

    # Dynamic Time Window Overrides

    env_start_time = os.environ.get("DIRECT_DATA_START_TIME")
    env_stop_time = os.environ.get("DIRECT_DATA_STOP_TIME")
    
    if env_start_time:
        direct_data_params['start_time'] = env_start_time
    if env_stop_time:
        direct_data_params['stop_time'] = env_stop_time
        
    # Auto-calculate last 24h if requested (simple incremental mode)
    if os.environ.get("USE_DYNAMIC_WINDOW") == "true":
        now = datetime.utcnow()
        # Look back 24 hours to ensure we catch the file (assuming logic picks latest)
        # Adjust this window based on your actual run frequency and needs
        start_time_dynamic = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        stop_time_dynamic = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        print(f"Using Dynamic Window: {start_time_dynamic} to {stop_time_dynamic}")
        direct_data_params['start_time'] = start_time_dynamic
        direct_data_params['stop_time'] = stop_time_dynamic

    s3_params: dict = config_params['s3']

    redshift_params: dict = config_params['redshift']

    extract_document_content: bool = config_params.get('extract_document_content')
    retrieve_document_text: bool = config_params.get('retrieve_document_text')

    object_storage_root: str = f's3://{s3_params["bucket_name"]}'

    s3_params['convert_to_parquet'] = config_params['convert_to_parquet']
    redshift_params['convert_to_parquet'] = config_params['convert_to_parquet']
    redshift_params['object_storage_root'] = object_storage_root

    s3_service: AwsS3Service = AwsS3Service(s3_params)
    redshift_service: RedshiftService = RedshiftService(redshift_params)
    vault_service: VaultService = VaultService(vapil_settings_filepath)
    
    # State Manager Initialization
    # Parameter name can be customized via env var
    state_param_name = os.environ.get("STATE_PARAMETER_NAME", "VeevaDirectDataWatermark")
    state_manager = StateManager(parameter_name=state_param_name)

    # Check for Watermark
    if os.environ.get("USE_DYNAMIC_WINDOW") == "true":
        last_processed_time = state_manager.get_last_processed_time()
        if last_processed_time:
            print(f"Found Watermark: {last_processed_time}. Using as start_time.")
            direct_data_params['start_time'] = last_processed_time
            # Keep stop_time as NOW (calculated above) or env override

    # Run Downloader (returns list of files)
    processed_files = direct_data_to_object_storage.run(vault_service=vault_service,
                                                      object_storage_service=s3_service,
                                                      direct_data_params=direct_data_params)

    if not processed_files:
        print("No files found to process.")
        return

    # Sort files by timestamp to ensure sequential processing
    # Assuming ISO format string sort works, or implement robust parsing if needed
    processed_files.sort(key=lambda x: x['timestamp'])

    for file_info in processed_files:
        print(f"--- Processing File: {file_info['filename']} ---")
        
        # Update S3 Service to point to this specific file
        s3_service.archive_filepath = file_info['object_path']
        # Derive extract folder from filename to keep them separate (optional but good for debugging)
        # Assuming filename is like '123_20230101_F.tar.gz', split off extension
        # Original logic likely stripped .tar.gz. 
        # download_and_unzip handles folder creation based on archive_filepath, so simple assignment is enough.
        
        # Download and Unzip from S3 to Local/S3
        download_and_unzip_direct_data_files.run(object_storage_service=s3_service)

        # Load Data to Redshift
        load_data.run(object_storage_service=s3_service,
                      database_service=redshift_service,
                      direct_data_params=direct_data_params)

        if extract_document_content:
            extract_doc_content.run(object_storage_service=s3_service,
                                    vault_service=vault_service)

        if retrieve_document_text:
            retrieve_doc_text.run(object_storage_service=s3_service,
                                  vault_service=vault_service)
                                  
        # Update Watermark
        state_manager.set_last_processed_time(file_info['timestamp'])
        print(f"--- Successfully Processed {file_info['filename']}. Watermark updated. ---")



if __name__ == "__main__":
    main()
