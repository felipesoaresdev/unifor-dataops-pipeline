import os
import traceback
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

def transform_and_store():
    try:
        print(">> Iniciando transform_and_store...")

        # Config MongoDB
        mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://felipesoares:9HfoY2kZVRS95HK4@unifor.sxibmbn.mongodb.net/?retryWrites=true&w=majority&appName=unifor")
        db_name = "tabnews_raw"
        collection_name = "raw_contents"

        print(">> Conectando ao MongoDB...")
        client = MongoClient(mongo_uri)
        collection = client[db_name][collection_name]

        print(">> Lendo documentos do MongoDB...")
        data = list(collection.find())
        if not data:
            print("⚠️ Nenhum documento encontrado.")
            return

        print(f">> {len(data)} documentos encontrados. Transformando...")
        df = pd.DataFrame(data)

        # Seleção de colunas
        expected_cols = [
            "id", "owner_id", "slug", "title", "status", "type",
            "source_url", "created_at", "updated_at", "published_at",
            "owner_username", "tabcoins", "tabcoins_credit", "tabcoins_debit",
            "children_deep_count", "_ingested_at"
        ]
        df = df[expected_cols].copy()

        # Conversões
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce")

        int_cols = ["tabcoins", "tabcoins_credit", "tabcoins_debit", "children_deep_count"]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        print("✅ Transformações aplicadas com sucesso.")

        # Config PostgreSQL
        pg_user = os.getenv("PG_USER", "datalake_user")
        pg_pass = os.getenv("PG_PASS", "datalake_pass")
        pg_host = os.getenv("PG_HOST", "datalake")
        pg_port = os.getenv("PG_PORT", "5432")
        pg_db   = os.getenv("PG_DB", "datalake")
        engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

        # Criação do schema se necessário
        schema_name = "staging_tabnews"
        print(f">> Garantindo existência do schema '{schema_name}'...")
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

        # Gravação na tabela
        print(">> Enviando dados para staging_tabnews.staging_news...")
        df.to_sql("staging_news", engine, schema=schema_name, if_exists="replace", index=False)

        print(f"✅ {len(df)} registros carregados com sucesso.")

    except Exception as e:
        print("❌ Erro no transform_and_store:")
        traceback.print_exc()

if __name__ == "__main__":
    transform_and_store()
