import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
from datetime import datetime
import logging
from azure.storage.fileshare import ShareClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Function execution started")
    current_date = datetime.utcnow().date()
    base_directory_path = "--"

    try:
        file_share_connection_string = os.getenv('SOURCE_STORAGE_CONNECTION_STRING')
        file_share = ShareClient.from_connection_string(conn_str=file_share_connection_string, share_name="--")
        logging.info("Connected to Azure File Share successfully.")
    except Exception as e:
        logging.error(f"Failed to connect to Azure File Share: {str(e)}")
        return func.HttpResponse(f"Error connecting to Azure File Share: {str(e)}", status_code=500)

    try:
        blob_connection_string = os.getenv('TARGET_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        container_client = blob_service_client.get_container_client("--")
    except Exception as e:
        logging.error(f"Failed to initialize storage clients: {str(e)}")
        return func.HttpResponse(f"Error initializing storage clients: {str(e)}", status_code=500)

    # Recursive function to traverse directories
    def transfer_files(directory_path):
        directory_client = file_share.get_directory_client(directory_path)
        for item in directory_client.list_directories_and_files():
            full_path = os.path.join(directory_path, item['name'])
            if item['is_directory']:
                transfer_files(full_path)  # Recurse into subdirectory
            else:
                transfer_file(item, directory_client, directory_path, container_client, full_path)

    def transfer_file(file_item, directory_client, directory_path, container_client, full_path):
        try:
            file_client = directory_client.get_file_client(file_item['name'])
            file_properties = file_client.get_file_properties()
            last_modified_file = file_properties.last_modified
            blob_path = full_path
            blob_client = container_client.get_blob_client(blob_path)

            if blob_client.exists():
                blob_properties = blob_client.get_blob_properties()
                last_modified_blob = blob_properties.last_modified
                if last_modified_file <= last_modified_blob:
                    logging.info(f"File {file_item['name']} already transferred and up-to-date, skipping.")
                    return

            file_stream = file_client.download_file()
            file_content = file_stream.readall()
            logging.info(f"Downloaded {file_item['name']} successfully.")
        except Exception as e:
            logging.error(f"Failed to download or check file {file_item['name']}: {str(e)}")
            return

        try:
            metadata = {'original_file_share': "---", 'original_path': full_path, 'date': current_date.strftime('%Y-%m-%d'), 'transferred': 'true'}
            blob_client.upload_blob(data=file_content, metadata=metadata, overwrite=True)
            logging.info(f"File {file_item['name']} transferred successfully.")
        except Exception as e:
            logging.error(f"Failed to upload file {file_item['name']} to Blob Storage: {str(e)}")

    # Start the recursive transfer
    transfer_files(base_directory_path)
    logging.info("Function execution finished successfully.")
    return func.HttpResponse("Function execution finished successfully.", status_code=200)
