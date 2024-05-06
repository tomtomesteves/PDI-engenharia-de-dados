from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook   
import logging

QUERY = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id;
"""


# Função que será chamada pela task
def query_sales():
    # Usar o PostgresHook para conectar na base de dados 'postgres'
    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(QUERY)
    sales = cursor.fetchall()
    for sale in sales:
        logging.info(sale)
    cursor.close()
    connection.close()

# Definindo os argumentos padrões da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'query_sales_dag',
    default_args=default_args,
    description='A simple DAG to query sales from PostgreSQL',
    # schedule_interval=timedelta(days=1),
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Definindo a task
run_query = PythonOperator(
    task_id='run_query_sales',
    python_callable=query_sales,
    dag=dag,
)

# Configuração do workflow
run_query
