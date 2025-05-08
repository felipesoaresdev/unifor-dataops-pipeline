from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Adiciona o diretório de scripts ao path
sys.path.append('/opt/airflow/scripts')

# Imports dos scripts
from ingest import ingest_news
from transform_and_store import run_transform_and_store

# Definição da DAG
with DAG(
    dag_id="news_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["news"],
) as dag:

    # Task 1: Ingestão
    ingest_task = PythonOperator(
        task_id="ingest_news_from_api",
        python_callable=ingest_news
    )

    # Task 2: Transformação + Armazenamento
    transform_and_store_task = PythonOperator(
        task_id="transform_and_store_news",
        python_callable=run_transform_and_store
    )

    # Ordem de execução
    ingest_task >> transform_and_store_task
