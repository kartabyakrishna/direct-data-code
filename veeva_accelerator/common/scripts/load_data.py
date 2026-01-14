import os
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow as pa

from common.services.database_service import DatabaseService
from common.services.object_storage_service import ObjectStorageService
from accelerators.redshift.services.redshift_service import RedshiftService
from common.utilities import log_message
from common.utilities import update_table_name_that_starts_with_digit
from common.utilities import convert_file_to_table


def handle_metadata_updates(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            metadata_updates: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata updates')

    starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"

    file_extension: str = '.csv'
    if database_service.convert_to_parquet:
        file_extension = '.parquet'
    metadata_updates_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_updates['file'].iloc[0])[0]}{file_extension}"

    # Check if the file exists in Object Storage
    object_storage_service.check_if_object_exists(object_path=metadata_updates_file_path)

    # Download the file from Object Storage to local
    object_storage_service.download_object_to_local(object_path=metadata_updates_file_path,
                                                    output_path=metadata_updates_file_path)

    metadata_updates_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_updates_file_path,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

    unique_extract_values = metadata_updates_table["extract"].unique()
    for extract in unique_extract_values:
        table_name: str = update_table_name_that_starts_with_digit(extract.split(".")[1])
        filtered_metadata = metadata_updates_table[metadata_updates_table["extract"] == extract]

        # Get column names for this extract
        columns_df = metadata_updates_table.loc[
            metadata_updates_table["extract"] == extract, ["column_name", "type", "length"]]

        if "id" in columns_df["column_name"].tolist():
            database_service.create_single_table(table_name=table_name,
                                                 filtered_metadata=filtered_metadata)
        else:
            existing_columns = database_service.retrieve_column_info(table_name=table_name)
            columns_to_add: list = []
            columns_to_modify: list = []

            for _, row in columns_df.iterrows():
                column_name = row["column_name"]
                if column_name not in existing_columns.keys():
                    columns_to_add.append(row)
                else:
                    new_column_type = row["type"].lower()
                    if new_column_type != "string":
                        columns_to_modify.append(row[['column_name', 'type', 'length']].to_dict())

            if len(columns_to_add) > 0:
                database_service.add_columns_to_table(columns_to_add=pd.DataFrame(columns_to_add),
                                                      table_name=table_name)

            if len(columns_to_modify) > 0:
                modify_column_statement = database_service.create_sql_str_column_definitions(
                    pd.DataFrame(columns_to_modify), False, is_modify=True, is_add=False)
                database_service.db_connection.execute_query(f"""
                            ALTER TABLE {database_service.schema}.{table_name} {modify_column_statement}
                        """)


def handle_metadata_deletes(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            metadata_deletes: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata deletes')

    starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"

    file_extension: str = '.csv'
    if database_service.convert_to_parquet:
        file_extension = '.parquet'
    metadata_deletes_file_path: str = f"{starting_directory}/{os.path.splitext(metadata_deletes['file'].iloc[0])[0]}{file_extension}"

    # Check if the file exists in Object Storage
    object_storage_service.check_if_object_exists(object_path=metadata_deletes_file_path)

    # Download the file from Object Storage to local
    object_storage_service.download_object_to_local(object_path=metadata_deletes_file_path,
                                                    output_path=metadata_deletes_file_path)

    metadata_deletes_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_deletes_file_path,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

    unique_extract_values = metadata_deletes_table["extract"].unique()

    for extract in unique_extract_values:
        table_name: str = extract.split(".")[1]

        # Get column names for this extract
        columns: list = metadata_deletes_table.loc[metadata_deletes_table["extract"] == extract, "column_name"].tolist()

        # Check if 'id' exists in the columns list
        if "id" in columns:
            # Drop the entire table if 'id' is a column
            database_service.drop_table(table_name=table_name)
        elif columns:
            database_service.drop_columns_from_table(table_name=table_name, columns=columns)


def handle_metadata_changes(object_storage_service: ObjectStorageService,
                            database_service: DatabaseService,
                            manifest_table: pd.DataFrame):
    log_message(log_level='Info',
                message=f'Handling metadata changes')

    metadata_filter: pd.DataFrame = manifest_table[manifest_table["extract"] == "Metadata.metadata"]
    metadata_deletes: pd.DataFrame = metadata_filter[metadata_filter["type"] == "deletes"]
    metadata_updates: pd.DataFrame = metadata_filter[metadata_filter["type"] == "updates"]

    # Handle deletes if any
    if not metadata_deletes.empty and int(metadata_deletes['records'].iloc[0]) > 0:
        handle_metadata_deletes(object_storage_service=object_storage_service,
                                database_service=database_service,
                                metadata_deletes=metadata_deletes)

    # Handle updates if any
    if not metadata_updates.empty and int(metadata_updates['records'].iloc[0]) > 0:
        handle_metadata_updates(object_storage_service=object_storage_service,
                                database_service=database_service,
                                metadata_updates=metadata_updates)


def process_manifest_row(database_service: DatabaseService,
                         object_storage_service: ObjectStorageService,
                         row: pd.Series,
                         extract_type: str):
    raw_table_name: str = row["extract"].split(".")[1]
    table_name: str = update_table_name_that_starts_with_digit(raw_table_name)
    filename: str = row['file']
    if database_service.convert_to_parquet:
        filename = filename.replace(".csv", ".parquet")

    if not (extract_type == "incremental" and "metadata" in table_name):
        load_data_into_tables(database_service=database_service,
                              object_storage_service=object_storage_service,
                              extract_type=extract_type,
                              table_name=table_name,
                              filename=filename,
                              expected_row_count=int(row.get('records', 0)))


def load_data_into_tables(database_service: DatabaseService,
                          object_storage_service: ObjectStorageService,
                          extract_type: str,
                          table_name: str,
                     filename: str,
                     expected_row_count: int = None):
    full_object_path: str = object_storage_service.get_full_object_path(filename=filename)
    relative_object_path: str = object_storage_service.get_relative_object_path(filename=filename)
    headers: list[str] | None = None
    if database_service.convert_to_parquet:
        headers = object_storage_service.get_headers_from_parquet_file(object_path=relative_object_path)
    else:
        headers = object_storage_service.get_headers_from_csv_file(object_path=relative_object_path)

    if extract_type in ["full", "log"]:
        database_service.load_full_or_log_data(table_name=table_name,
                                               object_path=full_object_path,
                                               headers=headers,
                                               expected_row_count=expected_row_count)
    if extract_type == "incremental":
        database_service.load_incremental_data(table_name=table_name,
                                               object_path=full_object_path,
                                               headers=headers,
                                               expected_row_count=expected_row_count)

# Thread-Safe Worker logic for Grouped Table (Delete + Update)
def process_table_group_safe(redshift_params: dict,
                             object_storage_service: ObjectStorageService,
                             extract_type: str,
                             starting_directory: str,
                             delete_row: pd.Series = None,
                             update_row: pd.Series = None):
    """
    Worker function for parallel execution of a SINGLE TABLE.
    Handles both Deletes and Updates for that table in a single atomic transaction.
    """
    try:
        # Instantiate fresh service (and connection) for this thread
        local_service = RedshiftService(redshift_params)
        local_service.db_connection.begin_transaction()
        
        # 1. Process Deletes (if any) - MUST happen before updates
        if delete_row is not None:
            if local_service.check_file_processed(delete_row['file']):
                log_message("Info", f"Skipping DELETE for {delete_row['file']} (Already Processed)")
                delete_row = None
            else:
                local_service.process_delete(row=delete_row, starting_directory=starting_directory)
                local_service.log_file_processed(delete_row['file'])

        # 2. Process Updates (if any)
        if update_row is not None:
            if local_service.check_file_processed(update_row['file']):
                log_message("Info", f"Skipping UPDATE for {update_row['file']} (Already Processed)")
                update_row = None
            else:
                process_manifest_row(database_service=local_service,
                                     object_storage_service=object_storage_service,
                                     row=update_row,
                                     extract_type=extract_type)
                local_service.log_file_processed(update_row['file'])
        
        return local_service
    except Exception as e:
        table_info = "Unknown"
        if delete_row is not None: table_info = delete_row.get('file', 'unknown_del')
        if update_row is not None: table_info = update_row.get('file', 'unknown_upd')
        log_message("Error", f"Error in worker thread for table group {table_info}", exception=e)
        raise e


def run(object_storage_service: ObjectStorageService, 
        database_service: DatabaseService, 
        direct_data_params: dict,
        redshift_params: dict = None):
    
    log_message(log_level='Info',
                message=f'---Executing load_data.py---')
    try:
        starting_directory: str = f"{object_storage_service.direct_data_folder}/{object_storage_service.extract_folder}"
        extract_type: str = direct_data_params['extract_type']

        database_service.db_connection.activate_cursor()
        database_service.db_connection.activate_cursor()
        database_service.check_if_schema_exists()
        database_service.create_processed_files_log_table()

        file_extension: str = '.csv'
        if database_service.convert_to_parquet:
            file_extension = '.parquet'

        # Retrieve the Manifest File from Object Storage
        manifest_filepath: str = f"{starting_directory}/manifest{file_extension}"
        object_storage_service.check_if_object_exists(object_path=manifest_filepath)
        object_storage_service.download_object_to_local(object_path=manifest_filepath, output_path=manifest_filepath)

        # Convert the manifest file to a table
        manifest_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=manifest_filepath,
                                                                        convert_to_parquet=database_service.convert_to_parquet)

        if extract_type in ["full", "log"]:
            # Retrieve the Metadata File from Object Storage
            metadata_filepath: str = (
                f"{starting_directory}/Metadata/metadata{file_extension}"
                if extract_type == "full"
                else f"{starting_directory}/metadata_full{file_extension}"
            )
            object_storage_service.check_if_object_exists(object_path=metadata_filepath)
            object_storage_service.download_object_to_local(metadata_filepath, metadata_filepath)

            # Convert the metadata file to a table
            metadata_table: pd.DataFrame | pa.Table = convert_file_to_table(file_path=metadata_filepath,
                                                                            convert_to_parquet=database_service.convert_to_parquet)

            # Create all tables in the database (DDL is fast, safe to do sequentially usually)
            database_service.create_all_tables(starting_directory=starting_directory, metadata_table=metadata_table)

        elif extract_type == "incremental":
            # DDL operations (Sequential for safety)
            handle_metadata_changes(object_storage_service=object_storage_service,
                                    database_service=database_service,
                                    manifest_table=manifest_table)
            
            # NOTE: We do NOT call delete_data_from_table here anymore.
            # Deletes are now pushed to the parallel execution block below.

        # --- PREPARE WORK PLAN (Group by Table) ---
        # We need to group deletions and updates by Table Name so they can be processed 
        # in the same transaction (Delete First -> Then Update) to avoid locking issues.
        
        work_plan = {} # { 'table_name': { 'delete': row, 'update': row } }

        # Helper to get table name from row
        def get_table_name(row, is_delete=False):
            raw = row["extract"].split(".")[1]
            if is_delete:
                raw = raw.replace('_deletes', '')
            return update_table_name_that_starts_with_digit(raw)

        # 1. Identify Deletes
        deletes_filter = manifest_table[(manifest_table["type"] == "deletes") & (manifest_table["records"] > 0)]
        for _, row in deletes_filter.iterrows():
            t_name = get_table_name(row, is_delete=True)
            if t_name not in work_plan: work_plan[t_name] = {}
            work_plan[t_name]['delete'] = row

        # 2. Identify Updates
        updates_filter = manifest_table[(manifest_table["type"] == "updates") & (manifest_table["records"] > 0)]
        for _, row in updates_filter.iterrows():
            t_name = get_table_name(row, is_delete=False)
            if t_name not in work_plan: work_plan[t_name] = {}
            work_plan[t_name]['update'] = row

        # --- PARALLEL EXECUTION START ---
        # We submit "Table Groups" to the executor.
        
        active_services = []
        with ThreadPoolExecutor(max_workers=5) as executor: 
            futures = []
            
            for table_name, tasks in work_plan.items():
                delete_row = tasks.get('delete')
                update_row = tasks.get('update')
                
                if redshift_params and direct_data_params['extract_type'] == 'incremental':
                    # Parallel Mode (Incremental Only - typically where we have splits)
                    futures.append(executor.submit(process_table_group_safe, 
                                                 redshift_params, 
                                                 object_storage_service, 
                                                 extract_type,
                                                 starting_directory,
                                                 delete_row,
                                                 update_row))
                else:
                    # Sequential Fallback (or Full Load/Log which might process differently, 
                    # but logic here unifies them safely. If params missing, use main service)
                    # Note: Legacy full load logic was simpler, but for safety lets execute sequentially if no parallel params
                    if delete_row is not None:
                         database_service.process_delete(delete_row, starting_directory)
                    if update_row is not None:
                         process_manifest_row(database_service, object_storage_service, update_row, extract_type)

            if futures:
                # Wait for all to complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        service = future.result()
                        active_services.append(service)
                    except Exception as e:
                        # If ANY thread fails, we must rollback ALL active transactions
                        log_message("Error", "Parallel load failed. Rolling back all threads.", exception=e)
                        for svc in active_services:
                            try:
                                svc.db_connection.rollback_transaction()
                                svc.db_connection.close()
                            except:
                                pass
                        raise e # Re-raise to trigger main failure handler

        # --- COORDINATED COMMIT ---
        # If we got here, all threads finished successfully.
        log_message("Info", "All parallel loads successful. Committing transactions...")
        for svc in active_services:
            svc.db_connection.commit_transaction()
            svc.db_connection.close()
            
        database_service.db_connection.close_cursor()
        database_service.db_connection.close()

    except Exception as e:
        log_message(log_level='Error',
                    message=f'Errors encountered when loading data.',
                    exception=e)
        raise e

