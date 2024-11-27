import json
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, ArrayType, DoubleType, StructField
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
from pymongo import MongoClient
from pyspark.sql.types import StructType, ArrayType, DoubleType, StructField, StringType, IntegerType, FloatType


# Créer une session Spark
spark = SparkSession.builder \
        .appName("Kafka Consumer") \
        .getOrCreate()

# Charger le modèle de classification RandomForest pré-entraîné
gbt_model = GBTClassificationModel.load("gbt_model2")

# Définir le schéma pour les messages Kafka
schema = StructType([
        StructField("scaled_features", ArrayType(DoubleType()), True),


        StructField("International plan", StringType(), True),
        StructField("Voice mail plan", StringType(), True),
        StructField("Number vmail messages", IntegerType(), True),
        StructField("Total day minutes", FloatType(), True),
    
        StructField("Total day charge", FloatType(), True),
        StructField("Total eve minutes", FloatType(), True),

        StructField("Total eve charge", FloatType(), True),
        StructField("Total night minutes", FloatType(), True),
       
        StructField("Total night charge", FloatType(), True),
        StructField("Total intl minutes", FloatType(), True),
       
        StructField("Total intl charge", FloatType(), True),
        StructField("Customer service calls", IntegerType(), True)
    ])

# Définir une fonction définie par l'utilisateur pour convertir un tableau de doubles en vecteur
to_vector_udf = udf(lambda scaled_features: Vectors.dense(scaled_features), VectorUDT())

# Lire le flux Kafka
kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "mon-topic") \
        .load()

# Analyser les données JSON provenant de Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Extraire les données JSON de la colonne "value"
parsed_json_df = parsed_df.select(from_json(col("value"), schema).alias("json_data"))

# Extraire les valeurs de l'objet JSON
extracted_df = parsed_json_df.select(
    "json_data.scaled_features",
    "json_data.International plan",
    "json_data.Voice mail plan",
    "json_data.Number vmail messages",
    "json_data.Total day minutes",
    "json_data.Total day charge",
    "json_data.Total eve minutes",
    "json_data.Total eve charge",
    "json_data.Total night minutes",
    "json_data.Total night charge",
    "json_data.Total intl minutes",
    "json_data.Total intl charge",
    "json_data.Customer service calls"
)

# Convertir la colonne 'scaled_features' en vecteur
extracted_df = extracted_df.withColumn("scaled_features", to_vector_udf(extracted_df["scaled_features"]))

# Faire des prédictions en temps réel avec le modèle pré-entraîné
predictions_df = gbt_model.transform(extracted_df)

# Sélectionner toutes les colonnes de l'objet JSON avec les prédictions
output_df = predictions_df.select("*")

# Afficher les données JSON avec les prédictions
query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Définir une fonction pour insérer les données dans MongoDB
def insert_into_mongodb(batch_df, batch_id):
    # Connecter à MongoDB
      # Connecter à MongoDB
    client = MongoClient("mongodb://192.168.43.197:27017/")
    db = client["Telecom1"]
    collection = db["prediction"]

    try:
        # Insérer les données dans MongoDB
        for row in batch_df.collect():
            data = {
                "international_plan": row["International plan"],
                "Voice_mail_plan": row["Voice mail plan"],
                "number_vmail_messages": row["Number vmail messages"],
                "Total day minutes": row["Total day minutes"],
                "Total day charge": row["Total day charge"],
                "total_eve_minutes": row["Total eve minutes"],
                "total_eve_charge": row["Total eve charge"],
                "Total night minutes": row["Total night minutes"],
                "Total night charge": row["Total night charge"],
                "Total intl minutes": row["Total intl minutes"],
                "Total intl charge": row["Total intl charge"],
                "customer_service_calls": row["Customer service calls"],
                "prediction": row["prediction"]
            }
            # Insérer l'objet JSON dans MongoDB
            collection.insert_one(data)

        # Afficher un message après l'insertion
        print("Données insérées dans MongoDB avec succès !")
    
    except Exception as e:
        # Afficher un message en cas d'échec
        print(f"Erreur lors de l'insertion des données dans MongoDB : {str(e)}")
        raise
    
    finally:
        # Déconnecter de MongoDB
        client.close()

# Définir la requête pour écrire les données dans MongoDB
write_query = output_df.writeStream \
    .outputMode("append") \
    .foreachBatch(insert_into_mongodb) \
    .start()

# Attendre la fin du streaming
write_query.awaitTermination()
query.awaitTermination()