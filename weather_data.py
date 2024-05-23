from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG('weather_api_dag', default_args=default_args, schedule='@daily')

fetch_weather = HttpOperator(
    task_id='fetch_weather_data',
    method='GET',
    http_conn_id='weather_api',  # Airflow connection ID for the weather API
    endpoint='/weather?q=mutare&appid=2bbbab50c7247b65d36fe69c40e1661f',  # API endpoint to fetch weather data
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag,
)

fetch_weather
