from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/scripts')
from ingest import ingest_news

with DAG(
    dag_id="news_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["news"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_news_from_api",
        python_callable=ingest_news
    )
