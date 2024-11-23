import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

load_dotenv()

data_lake_name = os.environ["DATA_LAKE_NAME"]
data_lake_key = os.environ["DATA_LAKE_KEY"]

data_lake_url_endpoint = f"https://{data_lake_name}.dfs.core.windows.net/"
storage_account_connection_string = f"DefaultEndpointsProtocol=https;AccountName={data_lake_name};AccountKey={data_lake_key};EndpointSuffix=core.windows.net"

# Create a blob client
blob_service_client = BlobServiceClient.from_connection_string(storage_account_connection_string)

def list_files(container_name, dir_path):
    
    # Create a DataLakeServiceClient object
    service_client = DataLakeServiceClient(account_url=data_lake_url_endpoint, credential=data_lake_key)

    # Get the file system client
    file_system_client = service_client.get_file_system_client(container_name)

    # Get the directory client
    directory_client = file_system_client.get_paths(dir_path)    

    # iterate over the files and store their names in a list
    file_list = []
    
    for file in directory_client:
        file_list.append(file.name)    

    return file_list

def get_filepath_from_lake(container_name: str, blob_path: str):

    # Define the expiry time (1 hour from now in this example)
    expiry_time = datetime.utcnow() + timedelta(hours=1)

    # Generate SAS token
    sas_token = generate_blob_sas(
        blob_service_client.account_name,
        container_name,
        blob_path,
        account_key=blob_service_client.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time
    )

    blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_path}?{sas_token}"

    return blob_url

def save_json_to_lake(container_name, file_path, json_data):

    try:

        file_path = file_path + ".json"

        blob_client = blob_service_client.get_blob_client(container_name, file_path)

        json_str = json.dumps(json_data, ensure_ascii=False).encode('utf-8')

        blob_client.upload_blob(json_str, overwrite=True)

        return 0

    except Exception as e:
        return e   
    
def download_content(container, file_path):
    
    # Create a DataLakeServiceClient object
    service_client = DataLakeServiceClient(account_url=data_lake_url_endpoint, credential=data_lake_key)

    # Get the file system client
    file_system_client = service_client.get_file_system_client(container)

    # Get the data lake file client
    file_client = file_system_client.get_file_client(file_path)

    # Download the content of the file into a variable
    download_stream = file_client.download_file()
    downloaded_content = download_stream.readall()

    return downloaded_content