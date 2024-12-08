import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
import pyodbc
from io import StringIO
import azure.functions as func

# Load environment variables
connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container_name = os.environ["BLOB_CONTAINER_NAME"]
endpoint = os.environ["LANGUAGE_ENDPOINT"]
api_key = os.environ["LANGUAGE_KEY"]
server = os.environ["SQL_SERVER"]
database = os.environ["SQL_DATABASE"]
username = os.environ["SQL_USERNAME"]
password = os.environ["SQL_PASSWORD"]
driver = '{ODBC Driver 18 for SQL Server}'

# Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Text Analytics Client
text_analytics_client = TextAnalyticsClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))

# Function to process the uploaded blob
def main(myblob: func.InputStream, context: func.Context):
    blob_name = myblob.name
    logging.info(f"Processing blob: {blob_name}")
    process_blob(blob_name)
    
# Function to process the uploaded blob
def process_blob(blob_name):
    try:
        # Read CSV file from Blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob()
        csv_data = blob_data.readall().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
    except Exception as e:
        logging.error(f"Error reading blob data: {e}")
        return

    try:
        # Analyze Sentiments
        documents = df['text'].tolist()
        results = text_analytics_client.analyze_sentiment(documents)
        df['predicted_sentiment'] = [result.sentiment for result in results]
        df['confidence_score'] = [
            max(result.confidence_scores.positive, result.confidence_scores.neutral, result.confidence_scores.negative)
            for result in results
        ]
    except Exception as e:
        logging.error(f"Error analyzing sentiment: {e}")
        return

    try:
        # Insert into SQL Database
        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
        )
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO Reviews (title, text) VALUES (?, ?)
                """, row['title'], row['text']
            )
            cursor.execute(
                """
                INSERT INTO Sentiments (review_id, predicted_sentiment, confidence_score)
                VALUES ((SELECT id FROM Reviews WHERE title = ? AND text = ?), ?, ?)
                """, row['title'], row['text'], row['predicted_sentiment'], row['confidence_score']
            )
            conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting data into SQL Database: {e}")
