from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from config import LANGUAGE_ENDPOINT, LANGUAGE_KEY

def get_text_analytics_client():
    return TextAnalyticsClient(endpoint=LANGUAGE_ENDPOINT, credential=AzureKeyCredential(LANGUAGE_KEY))

def analyze_sentiment_in_batches(documents, batch_size=10):
    client = get_text_analytics_client()
    all_results = []
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        try:
            response = client.analyze_sentiment(batch)
            all_results.extend(response)
        except Exception as e:
            print("Une erreur s'est produite lors de l'analyse des sentiments:", e)
            return []
    return all_results
