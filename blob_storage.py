from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
from config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME

def get_blob_client(container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client.get_blob_client(blob_name)

def read_blob_to_dataframe(blob_name):
    blob_client = get_blob_client(BLOB_CONTAINER_NAME, blob_name)
    blob_data = blob_client.download_blob()
    csv_data = blob_data.readall().decode('utf-8')
    return pd.read_csv(StringIO(csv_data))
