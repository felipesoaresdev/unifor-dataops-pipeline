from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType

# Cria a SparkSession com suporte ao MongoDB
spark = SparkSession.builder \
    .appName("ReadMongoDB") \
    .config("spark.mongodb.input.uri", "mongodb+srv://felipesoares:9HfoY2kZVRS95HK4@unifor.sxibmbn.mongodb.net/tabnews_raw.raw_contents") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Lê os dados do MongoDB
df = spark.read.format("mongo").load()

# Converte os campos com problemas de tipo
df = df \
    .withColumn("parent_id", col("parent_id").cast(StringType())) \
    .withColumn("deleted_at", col("deleted_at").cast(StringType())) \
    .withColumn("tabcoins", col("tabcoins").cast(IntegerType())) \
    .withColumn("tabcoins_credit", col("tabcoins_credit").cast(IntegerType())) \
    .withColumn("tabcoins_debit", col("tabcoins_debit").cast(IntegerType())) \
    .withColumn("children_deep_count", col("children_deep_count").cast(IntegerType()))

# Exibe os dados (opcional)
df.show(5, truncate=False)

# Salva em Parquet
df.write.mode("overwrite").parquet("/tmp/tabnews_raw.parquet")

# Finaliza a sessão
spark.stop()
