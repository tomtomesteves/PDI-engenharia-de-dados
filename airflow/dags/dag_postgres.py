from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


def extract_data_and_save_to_csv():

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id', schema='loja_carros')
    
    sql_query = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id;
    """

    result_df = pg_hook.get_pandas_df(sql_query)
    print(result_df)

    result_df.to_csv("/tmp/vendas.csv", index=False)


def process_csv_and_print_ranking():

    df = pd.read_csv("/tmp/vendas.csv")
    
    vendedor_sales = df.groupby("funcionario_nome")["preco_de_venda"].sum().reset_index()
    
    vendedor_sales_sorted = vendedor_sales.sort_values(by="preco_de_venda", ascending=False).reset_index(drop=True)

    for i, row in vendedor_sales_sorted.iterrows():
        print(f"Vendedor {row['funcionario_nome']} faturou o valor {row['preco_de_venda']} e ficou em {i+1}º lugar.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('postgres_connection_dag', default_args=default_args, description='DAG with Postgres connection', schedule_interval='@daily', start_date=datetime(2024, 5, 1), catchup=False) as dag:
    
    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_and_save_to_csv,
        provide_context=True,
    )
    
    process_csv_task = PythonOperator(
        task_id='process_csv_task',
        python_callable=process_csv_and_print_ranking,
        provide_context=True,
    )
    
    extract_data_task >> process_csv_task
