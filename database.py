import pyodbc
from config import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD

def get_sql_connection():
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER};PORT=1433;DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
    return pyodbc.connect(connection_string)

def insert_data_into_table(cursor, query, data):
    cursor.execute(query, data)
    cursor.commit()

def insert_reviews_and_sentiments(df_without_labels):
    conn = get_sql_connection()
    cursor = conn.cursor()

    batch_size = 1000
    for start in range(0, len(df_without_labels), batch_size):
        batch = df_without_labels.iloc[start:start + batch_size]
        
        # Insertion dans la table Reviews
        for _, row in batch.iterrows():
            query = """
            INSERT INTO Reviews (title, text)
            VALUES (?, ?)
            """
            insert_data_into_table(cursor, query, (row['title'], row['text']))
        
        # Insertion des sentiments dans la table Sentiments
        for _, row in batch.iterrows():
            query = """
            INSERT INTO Sentiments (review_id, predicted_sentiment)
            VALUES ((SELECT id FROM Reviews WHERE title = ? AND text = ?), ?)
            """
            insert_data_into_table(cursor, query, (row['title'], row['text'], row['predicted_sentiment']))
        
        # Insertion des scores dans la table Scores
        for _, row in batch.iterrows():
            query = """
            INSERT INTO Scores (sentiment_id, confidence_score)
            VALUES ((SELECT id FROM Sentiments WHERE review_id = (SELECT id FROM Reviews WHERE title = ? AND text = ?)), ?)
            """
            insert_data_into_table(cursor, query, (row['title'], row['text'], row['confidence_score']))

    cursor.close()
    conn.close()
    print("Insertion terminée dans la base de données.")
