from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def print_and_return_ds_ts(**kwargs):
    ds = kwargs['ds']
    ts = kwargs['ts']
    print(f"Data de execução (DS): {ds}")
    print(f"Timestamp de execução (TS): {ts}")
    return {'ds': ds, 'ts': ts}

def concatenate_ds_ts(**kwargs):
    ds = kwargs['ti'].xcom_pull(task_ids='python_task_1')['ds']
    ts = kwargs['ti'].xcom_pull(task_ids='python_task_2')['ts']
    result = f"{ds} -- {ts}"
    print("Resultado concatenado:", result)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('multi_task_dag', default_args=default_args, description='A DAG with multiple tasks', schedule_interval='@daily', start_date=days_ago(7), catchup=False) as dag:

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "DAG iniciada"'
    )

    python_task_1 = PythonOperator(
        task_id='python_task_1',
        python_callable=print_and_return_ds_ts,
        provide_context=True,
    )

    python_task_2 = PythonOperator(
        task_id='python_task_2',
        python_callable=print_and_return_ds_ts,
        provide_context=True,
    )

    dummy_task = EmptyOperator(
        task_id='dummy_task'
    )

    concatenate_task = PythonOperator(
        task_id='concatenate_task',
        python_callable=concatenate_ds_ts,
        provide_context=True,
    )

    bash_task >> [python_task_1, python_task_2] >> dummy_task >> concatenate_task
