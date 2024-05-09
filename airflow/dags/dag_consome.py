from datetime import timedelta
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.utils.dates import days_ago

filepath = '/tmp/dados_vendas.csv'
dataset = Dataset(uri=filepath)

def process_csv_and_print_ranking():
    df = pd.read_csv("/tmp/dados_vendas.csv")
    
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

with DAG('processamento_analise_vendas',
         default_args=default_args,
         description='Análise de vendas por vendedor',
         schedule=[dataset],
         start_date=days_ago(7),
         catchup=False) as dag:

    process_csv_and_print_ranking = PythonOperator(
        task_id='process_csv_and_print_ranking',
        python_callable=process_csv_and_print_ranking,
        dag=dag
    )
