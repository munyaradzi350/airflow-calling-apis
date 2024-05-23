from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


default_args = {
    'owner': 'CodeData',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    message = f"Hello World! My name is {first_name} {last_name}, and I am {age} years old!"
    print(message)
    return message

def get_name(ti):
    ti.xcom_push(key='first_name', value='Wilson')
    ti.xcom_push(key='last_name', value='Kumalo')

def get_age(ti):
     ti.xcom_push(key='age', value=19)   

def error_callback(context):
    error_message = f"Error occurred: {context['exception']}"
    print(error_message)
    

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_operator_v12',
    description='Creating this dag using python operator',
    start_date=datetime(2024, 4, 29),
    schedule_interval='@daily',
    catchup=False,  # Prevent backfilling for past dates
    on_failure_callback=error_callback  
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age': 20},
        provide_context=True  # Enable passing context to the callable function
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2, task3] >> task1
