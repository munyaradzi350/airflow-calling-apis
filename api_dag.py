import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

# Import the datetime module correctly
import datetime as dt

def save_posts(ti) -> None:
     posts = ti.xcom_pull(task_ids=['get_posts'])
     with open('/Users/munya/airflow/data/post.json', 'w') as f:
          json.dump(posts[0], f)

# Define your DAG
with DAG(
    dag_id='api_dag',
    schedule='@daily',
    start_date=datetime(2024, 5, 2),
    catchup=False
) as dag:
     
     # Define the HttpSensor task
     task_is_api_active = HttpSensor(
          task_id='is_api_active',
          http_conn_id='api_post',  
          endpoint='posts/'
     )

     # Define the SimpleHttpOperator task
     task_get_posts = SimpleHttpOperator(
          task_id='get_posts',
          http_conn_id='api_post',
          endpoint='posts/',
          method='GET',
          response_filter=lambda response: json.loads(response.text),
          log_response=True
     )

     task_save = PythonOperator(
          task_id='save_post',
          python_callable=save_posts
     )
