from pyspark.sql import SparkSession

# Cria sessão Spark
spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .getOrCreate()

# Lê o Parquet salvo anteriormente
df = spark.read.parquet("/tmp/tabnews_raw.parquet")

# Mostra schema para análise (útil em caso de erro)
df.printSchema()
df.show(1, truncate=False)

# Remove colunas com tipos complexos que o PostgreSQL não aceita via JDBC
from pyspark.sql.types import StructType, ArrayType, MapType

simple_columns = [
    field.name
    for field in df.schema.fields
    if not isinstance(field.dataType, (StructType, ArrayType, MapType))
]

df_flat = df.select(*simple_columns)

# Configurações JDBC para PostgreSQL
jdbc_url = "jdbc:postgresql://datalake:5432/datalake"
properties = {
    "user": "datalake_user",
    "password": "datalake_pass",
    "driver": "org.postgresql.Driver"
}

# Escreve os dados no PostgreSQL (schema público, tabela raw_contents)
df_flat.write.jdbc(
    url=jdbc_url,
    table="raw_contents",
    mode="overwrite",
    properties=properties
)
