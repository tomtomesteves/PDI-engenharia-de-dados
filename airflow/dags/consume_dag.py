from datetime import datetime
from airflow.decorators import dag, task
from airflow import Dataset
import csv
from collections import defaultdict

filepath = '/tmp/sales_data.csv'
dataset = Dataset(uri=filepath)

@dag(
    schedule=[dataset],
    start_date=datetime(2023, 1, 2),  # Um dia após a primeira DAG
    catchup=False,
    tags=['dataset_example']
)
def calculate_average_sales_dag():
    @task
    def read_csv_and_calculate_averages():
        sales_dict = defaultdict(list)
        try:
            with open(dataset.uri, 'r', newline='') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    name = row['funcionario_nome']
                    price = float(row['preco_de_venda'])
                    sales_dict[name].append(price)
            
            averages = {name: sum(prices) / len(prices) for name, prices in sales_dict.items()}
            for name, avg in averages.items():
                print(f"{name}: Average Sale = {avg:.2f}")
        except Exception as e:
            print(f"Failed to read or process the CSV file: {e}")

    read_csv_and_calculate_averages()

dag2 = calculate_average_sales_dag()
