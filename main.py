import pandas as pd
from blob_storage import read_blob_to_dataframe
from text_analytics import analyze_sentiment_in_batches
from database import insert_reviews_and_sentiments

# Lire les fichiers depuis Azure Blob Storage
df_with_labels = read_blob_to_dataframe("data_with_labels.csv")
df_without_labels = read_blob_to_dataframe("data_without_labels.csv")

# Analyser les sentiments des commentaires sans labels
documents_without_labels = df_without_labels['text'].tolist()
results = analyze_sentiment_in_batches(documents_without_labels)

# Ajouter les résultats d'analyse au DataFrame
df_without_labels['predicted_sentiment'] = [result.sentiment for result in results]

# Ajouter les scores de confiance
def get_confidence_score(sentiment):
    if sentiment == 'positive' or sentiment == 'negative':
        return 1
    elif sentiment == 'neutral' or sentiment == 'mixed':
        return 0.5
    else:
        return 0

df_without_labels['confidence_score'] = df_without_labels['predicted_sentiment'].apply(get_confidence_score)

# Compareration des résultats de l'analyse avec les labels

# Réinitialiser les indices des DataFrames pour s'assurer que les indices correspondent
df_with_labels.reset_index(drop=True, inplace=True)
df_without_labels.reset_index(drop=True, inplace=True)

# Comparer les sentiments prédits avec les labels réels (1: négatif, 2: positif)
def compare_predictions(row, predicted_sentiment):
    actual_label = row['label']
    if (actual_label == 1 and predicted_sentiment == 'negative') or (actual_label == 2 and predicted_sentiment == 'positive'):
        return 1  # Prédiction correcte
    elif predicted_sentiment == 'neutral' or predicted_sentiment == 'mixed':
        return 0  # Prédiction neutral/mixed, non pris en compte pour la précision stricte
    else:
        return 'wrong'  # Prédiction incorrecte

# Comparer les sentiments dans les deux DataFrames
df_with_labels['predicted_sentiment'] = df_without_labels['predicted_sentiment']
df_with_labels['comparison'] = df_with_labels.apply(
    lambda row: compare_predictions(row, row['predicted_sentiment']),
    axis=1
)

# Afficheage les résultats de la comparaison
print("Comparaison des résultats:")
print(df_with_labels[['label', 'predicted_sentiment', 'comparison']].head())

# Afficher un échantillon des résultats pour analyser la distribution des prédictions
print(df_without_labels[['predicted_sentiment']].head(20))

# Calcul de la précision des prédictions

from sklearn.metrics import accuracy_score

# Calcul de la précision avec poids pour les sentiments "neutral" et "mixed"
def calculate_weighted_accuracy(df_with_labels, df_without_labels):
    correct_predictions = 0
    total_predictions = 0
    weighted_sum = 0

    # Assigner les sentiments prédits à df_with_labels
    df_with_labels['predicted_sentiment'] = df_without_labels['predicted_sentiment']

    for index, row in df_with_labels.iterrows():
        true_label = row['label']
        predicted_sentiment = row['predicted_sentiment']

        # Poids de précision par sentiment
        weight = 1  # Poids par défaut pour une prédiction correcte (positive ou negative)
        
        # Si le sentiment est "neutral" ou "mixed", on applique un poids faible pour ne pas affecter lourdement la précision
        if predicted_sentiment in ['neutral', 'mixed']:
            weight = 0.5  # Poids faible pour neutral ou mixed, vous pouvez ajuster ce poids
        
        # Comparaison des prédictions
        if (true_label == 1 and predicted_sentiment == 'negative') or (true_label == 2 and predicted_sentiment == 'positive'):
            correct_predictions += weight
        total_predictions += weight

    # Calcul de la précision pondérée
    weighted_accuracy = correct_predictions / total_predictions
    return weighted_accuracy

weighted_accuracy = calculate_weighted_accuracy(df_with_labels, df_without_labels)

# Affichage de la précision
print(f"Précision pondérée du modèle : {weighted_accuracy:.4f}")


# Insérer les données dans la base de données SQL
insert_reviews_and_sentiments(df_without_labels)

print("Traitement terminé.")
