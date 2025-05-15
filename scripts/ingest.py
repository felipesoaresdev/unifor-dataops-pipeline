import requests
from pymongo import MongoClient
from datetime import datetime, timezone

def ingest_news():
    # 1. Consumir a API do TabNews
    url = "https://www.tabnews.com.br/api/v1/contents?page=1&per_page=10&strategy=new"
    response = requests.get(url)
    data = response.json()

    # 2. Conectar ao MongoDB Atlas
    mongo_uri = "mongodb+srv://felipesoares:password@unifor.sxibmbn.mongodb.net/?retryWrites=true&w=majority&appName=unifor"
    client = MongoClient(mongo_uri)
    db = client["tabnews_raw"]
    collection = db["raw_contents"]

    # 3. Adicionar campo de timestamp e inserir no MongoDB
    for item in data:
        item["_ingested_at"] = datetime.now(timezone.utc)
        collection.insert_one(item)

    print("✅ Dados brutos inseridos no MongoDB com sucesso.")
