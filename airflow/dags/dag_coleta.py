from datetime import timedelta
from airflow import DAG, Dataset
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

filepath = '/tmp/dados_vendas.csv'
dataset = Dataset(uri=filepath)

def consulta_banco_e_cria_dataset():

    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')

    sql_query = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id;
    """

    resultado = postgres_hook.get_pandas_df(sql_query)

    resultado.to_csv(filepath, index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('consulta_banco_e_cria_dataset',
         default_args=default_args,
         description='Consulta ao banco e criação de dataset',
         schedule_interval=None,
         start_date=days_ago(7),
         catchup=False) as dag:

    consulta_banco_e_cria_dataset = PythonOperator(
        task_id='consulta_banco_e_cria_dataset',
        python_callable=consulta_banco_e_cria_dataset,
        dag=dag,
        outlets = [dataset]
    )
