from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
import json

default_args = {
    'owner': 'airflow-munya',
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
}

with DAG(
    dag_id='fetch_jobs', 
    default_args=default_args, 
    schedule_interval='@daily'
) as dag:
    
    task_fetch_job_postings = SimpleHttpOperator(
    task_id='fetch_job_postings',
    method='GET',
    http_conn_id='odoo_api_connection',  # Airflow connection to Odoo API
    endpoint='jobs/get_job_postings',  # Example endpoint to fetch job postings
    headers={'Authorization': 'Bearer 809137deea3d13819348df773e7371201361dfcd'},
    dag=dag,
    response_filter=lambda response: json.loads(response.text),
    log_response=True
)

##airflow tasks test fetch_jobs fetch_job_postings 2024-05-09 

