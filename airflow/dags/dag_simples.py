from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def print():
    return 'Estou gostando muito do PDI!'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='Dag simples para a primeira etapa do trabalho',
    schedule_interval=timedelta(days=1)
) as dag:

# Defina os operadores
    start_operator = EmptyOperator(task_id='Tarefa_inicio')
    hello_operator = PythonOperator(task_id='Printar_frase', python_callable=print)
    end_operator = EmptyOperator(task_id='Tarefa_fim')

start_operator >> hello_operator >> end_operator    
