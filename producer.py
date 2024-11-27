from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier,NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from confluent_kafka import Producer
import json
import pandas as pd
import json
from kafka import KafkaProducer
import time
import logging
from pyspark.ml.feature import MinMaxScaler

# Initialiser SparkSession
spark = SparkSession.builder \
    .appName("Prétraitement et ML avec PySpark") \
    .getOrCreate()

# Charger les données
file_path = "churn-bigml-20.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)


# Convertir la colonne "Churn" en StringType
df = df.withColumn("Churn", df["Churn"].cast("string"))

# Encoder la variable cible "Churn"
indexer = StringIndexer(inputCol="Churn", outputCol="label")
df = indexer.fit(df).transform(df)

# Sélectionner les colonnes pour les caractéristiques
# Sélectionner les colonnes pour les caractéristiques
feature_cols = ['International plan', 'Voice mail plan', 'Number vmail messages',
       'Total day minutes', 'Total day charge', 'Total eve minutes',
       'Total eve charge', 'Total night minutes', 'Total night charge',
       'Total intl minutes', 'Total intl charge', 'Customer service calls']

# Supprimer les valeurs manquantes
df = df.na.drop()

# Encoder les variables catégoriques en numériques
indexers = [StringIndexer(inputCol=col, outputCol=col+"_index").fit(df) for col in ['State', 'International plan', 'Voice mail plan']]
pipeline = Pipeline(stages=indexers)
df = pipeline.fit(df).transform(df)
assembler = VectorAssembler(inputCols=[col+"_index" if col in ['State', 'International plan', 'Voice mail plan'] else col for col in feature_cols],
                            outputCol="features")
df = assembler.transform(df)
df1=df.select("features")
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
# Calculer les statistiques de résumé et normaliser les caractéristiques
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)
df = df.select("International plan", "Voice mail plan", "Number vmail messages",
               "Total day minutes", "Total day charge", "Total eve minutes",
               "Total eve charge", "Total night minutes", "Total night charge",
               "Total intl minutes", "Total intl charge", "Customer service calls", "scaled_features")

producer = KafkaProducer(bootstrap_servers=['kafka:9092'], max_block_ms=5000, api_version=(2, 5, 0))

while True:
    try:
        # Traitez chaque ligne du DataFrame
        for row in df.collect():
            # Créez un dictionnaire pour stocker les valeurs de toutes les colonnes
            message = {}
            message["scaled_features"] = row.scaled_features.toArray().tolist()
            message["International plan"] = row["International plan"]
            message["Voice mail plan"] = row["Voice mail plan"]
            message["Number vmail messages"] = row["Number vmail messages"]
            message["Total day minutes"] = row["Total day minutes"]
            message["Total day charge"] = row["Total day charge"]
            message["Total eve minutes"] = row["Total eve minutes"]
            message["Total eve charge"] = row["Total eve charge"]
            message["Total night minutes"] = row["Total night minutes"]
            message["Total night charge"] = row["Total night charge"]
            message["Total intl minutes"] = row["Total intl minutes"]
            message["Total intl charge"] = row["Total intl charge"]
            message["Customer service calls"] = row["Customer service calls"]
            
            # Convertissez le dictionnaire en JSON
            json_message = json.dumps(message)
            
            # Envoyez le message à Kafka
            producer.send('mon-topic', json_message.encode('utf-8'))
            
            print("Message envoyé:", json_message)
            
            time.sleep(5)  # Ajoutez un petit délai entre l'envoi de chaque ligne
    except Exception as e:
        logging.error(f'Une erreur s\'est produite: {e}')
        continue









