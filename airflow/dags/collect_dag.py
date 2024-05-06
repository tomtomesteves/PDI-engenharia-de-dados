from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook   
import csv


filepath = '/tmp/sales_data.csv'
dataset = Dataset(uri=filepath)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='export_sales_data_dag',
    default_args=default_args,
    description='A DAG to export sales data to CSV',
    schedule_interval=None,
    catchup=False,
    tags=['dataset_example']
):

    @task(outlets=[dataset])
    def export_sales_to_csv():
        pg_hook = PostgresHook(postgres_conn_id='postgres_db')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        query = """
        SELECT f.nome AS funcionario_nome, v.preco_de_venda
        FROM vendas v
        JOIN funcionarios f ON v.funcionario_id = f.id;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        # Caminho para salvar o arquivo CSV no disco local do worker
        with open(filepath, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['funcionario_nome', 'preco_de_venda'])
            writer.writerows(results)

        cursor.close()
        connection.close()
        
    export_sales_to_csv()
