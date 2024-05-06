from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging


def task_1(ti, ts):
    ti.xcom_push(key="ts_inicial", value=ts)

def task_2(ti):
    ts = ti.xcom_pull(key="ts_inicial")
    logging.info(ts+"a")

def executa_consulta(ds):
    query = f"select * from sales where date > {ds}"

args = {
    'owner': 'sales',
    'email_on_retry': None,
    'email_on_failure': None,
    'retry': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="teste_varias_tasks",
    schedule_interval=timedelta(days=1),    
    catchup=True,
    start_date=datetime(2024, 4, 1),
    default_args=args,
    tags=["sales"]
) as dag:
    
    primeira_task = PythonOperator(
        task_id="task_1",
        python_callable=task_1
    )

    segunda_task = PythonOperator(
        task_id="task_2",
        python_callable=task_2
    )

    primeira_task >> segunda_task