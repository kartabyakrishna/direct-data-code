import sys
import io

sys.path.append('.')
from common.utilities import log_message
from common.api.model.response.direct_data_response import DirectDataResponse
from common.api.model.response.vault_response import VaultResponse
from common.services.object_storage_service import ObjectStorageService
from common.services.vault_service import VaultService


def _handle_multipart_upload(object_storage_service: ObjectStorageService, vault_service: VaultService,
                             direct_data_item: DirectDataResponse.DirectDataItem, object_path: str):
    log_message(log_level='Info',
                message=f'Handling multipart upload')

    multipart_upload_response: dict | None = {}

    try:
        multipart_upload_response = object_storage_service.create_multipart_upload(
            object_path=object_path)

        upload_parts: list = []

        for file_part in direct_data_item.filepart_details:
            download_response: VaultResponse = vault_service.download_direct_data_file(
                name=file_part.name)

            part_info: dict = object_storage_service.upload_part(
                object_path=object_path,
                multipart_upload_response=multipart_upload_response,
                part_number=file_part.filepart,
                data=download_response.binary_content)

            upload_parts.append(part_info)

        object_storage_service.complete_multipart_upload(
            object_path=object_path,
            multipart_upload_response=multipart_upload_response,
            parts=upload_parts)

        log_message(log_level='Info',
                    message=f'Multipart upload completed')

    except Exception as e:
        # Abort the multipart upload in case of an error
        object_storage_service.abort_multipart_upload(
            object_path=object_path,
            multipart_upload_response=multipart_upload_response)

        log_message(log_level='Error',
                    message=f'Multipart upload aborted',
                    exception=e)
        raise e


def run(vault_service: VaultService, object_storage_service: ObjectStorageService, direct_data_params: dict):
    log_message(log_level='Info',
                message=f'---Executing direct_data_to_object_storage.py---')
    try:
        # List the Direct Data files of the specified extract type and time window
        extract_type: str = f"{direct_data_params['extract_type']}_directdata"
        start_time: str = direct_data_params['start_time']
        stop_time: str = direct_data_params['stop_time']
        list_direct_data_files_response: DirectDataResponse = vault_service.retrieve_available_direct_data_files(
            extract_type=extract_type,
            start_time=start_time,
            stop_time=stop_time
        )

        # Download ALL Direct Data files in the response, and upload to file storage.
        processed_files = []

        # Iterate through all available files (they are typically ordered by time)
        for direct_data_item in list_direct_data_files_response.data:
            log_message(log_level='Info', message=f"Processing file: {direct_data_item.filename} (Stop Time: {direct_data_item.stop_time})")


            # Exit if there are no records in the Direct Data extract (skip this file)
            if direct_data_item.record_count == 0:
                log_message(log_level='Info',
                            message=f'No records in the Direct Data extract for {direct_data_item.filename}.')
                continue

            # Put Direct Data file to Object Storage if there is only one file part
            object_path: str = f"{object_storage_service.direct_data_folder}/{direct_data_item.filename}"
            if direct_data_item.fileparts == 1:

                download_response: VaultResponse = vault_service.download_direct_data_file(
                    name=direct_data_item.filepart_details[0].name)

                object_storage_service.upload_object(
                    object_path=object_path,
                    data=io.BytesIO(download_response.binary_content)
                )

            # Create multi-part upload if Direct Data File has multiple parts
            else:
                _handle_multipart_upload(object_storage_service=object_storage_service,
                                         vault_service=vault_service,
                                         direct_data_item=direct_data_item,
                                         object_path=object_path)

            # Add to processed list
            processed_files.append({
                "filename": direct_data_item.filename,
                "timestamp": direct_data_item.stop_time, 
                "object_path": object_path
            })

            
        return processed_files



    except Exception as exception:
        log_message(log_level='Error',
                    message=f'Error retrieving Direct Data files from Vault'
                            f' and uploading to Object Storage',
                    exception=exception)
