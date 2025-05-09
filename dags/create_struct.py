from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Adiciona o diretório de scripts ao path
sys.path.append('/opt/airflow/scripts')

# Imports dos scripts
from create_dw import create_dw


# Definição da DAG
with DAG(
    dag_id="Create_struct",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["news"],
) as dag:


    # Task 1: criando estrutura dw
    create_dw_task = PythonOperator(
        task_id="criando_dw",
        python_callable=create_dw
    )

    # Ordem de execução
    create_dw_task
