import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
import os
import traceback

print(">> Iniciando script transform_and_store")

def run_transform_and_store():
    try:
        # Config MongoDB
        mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://felipesoares:9HfoY2kZVRS95HK4@unifor.sxibmbn.mongodb.net/?retryWrites=true&w=majority&appName=unifor")
        db_name = "tabnews_raw"
        collection_name = "raw_contents"

        print(">> Conectando ao MongoDB...")
        client = MongoClient(mongo_uri)
        collection = client[db_name][collection_name]

        print(">> Lendo documentos da coleção...")
        data = list(collection.find())

        if not data:
            print(">> Nenhum dado encontrado no MongoDB.")
            return

        print(f">> {len(data)} documentos encontrados. Transformando...")

        df = pd.DataFrame(data)

        df = df[[
            "id", "owner_id", "slug", "title", "status", "type",
            "source_url", "created_at", "updated_at", "published_at",
            "owner_username", "tabcoins", "tabcoins_credit", "tabcoins_debit",
            "children_deep_count", "_ingested_at"
        ]].copy()

        # Conversões de tipos
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["updated_at"] = pd.to_datetime(df["updated_at"])
        df["published_at"] = pd.to_datetime(df["published_at"])
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

        df["tabcoins"] = df["tabcoins"].astype(int)
        df["tabcoins_credit"] = df["tabcoins_credit"].astype(int)
        df["tabcoins_debit"] = df["tabcoins_debit"].astype(int)
        df["children_deep_count"] = df["children_deep_count"].astype(int)

        print(">> Transformações aplicadas com sucesso.")

        # Conexão PostgreSQL
        pg_user = "datalake_user"
        pg_pass = "datalake_pass"
        pg_host = "datalake"
        pg_port = "5432"
        pg_db   = "datalake"

        print(f">> Conectando ao PostgreSQL em {pg_host}:{pg_port}/{pg_db}...")
        engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

        print(">> Enviando dados para schema 'staging_tabnews' na tabela 'staging_news'...")
        df.to_sql("staging_news", engine, schema="staging_tabnews", if_exists="replace", index=False)
        print(f">> {len(df)} registros carregados com sucesso em staging_tabnews.staging_news.")

    except Exception:
        print("❌ Erro durante a execução:")
        traceback.print_exc()

# Executar diretamente
if __name__ == "__main__":
    run_transform_and_store()
