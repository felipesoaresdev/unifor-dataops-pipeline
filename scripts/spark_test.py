from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Spark Test") \
    .getOrCreate()

# Criando um DataFrame de teste
data = [("Felipe", 1), ("Maria", 2)]
df = spark.createDataFrame(data, ["nome", "id"])

# Exibindo o DataFrame
df.show()

spark.stop()
