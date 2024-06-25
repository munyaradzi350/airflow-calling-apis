from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import base64
import json

# Basic authentication credentials
username = 'admin'
password = 'Admin123'
auth_string = f'{username}:{password}'
base64_auth_string = base64.b64encode(auth_string.encode()).decode()

# Session cookie (if needed)
session_cookie = ''

default_args = {
    'owner': 'airflow-munya',
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def filter_visits(response, **context):
    print(f"Raw API Response: {response}")  # Debug: Print raw response
    if not response:
        print("No response received from the API.")
        return
    
    try:
        all_visits = json.loads(response)
    except json.JSONDecodeError:
        print("Failed to decode JSON response.")
        return

    current_time = datetime.utcnow()
    one_hour_ago = current_time - timedelta(hours=1)
    new_visits = [
        visit for visit in all_visits 
        if 'startDatetime' in visit and datetime.strptime(visit['startDatetime'], '%Y-%m-%dT%H:%M:%S.%fZ') > one_hour_ago
    ]
    
    if new_visits:
        print("New visits created in the past hour:")
        print(new_visits)
        # Optionally, push the result to XCom for downstream tasks
        context['ti'].xcom_push(key='new_visits', value=new_visits)
    else:
        print("No new visits for the past hour")

with DAG(
    dag_id='fetch_and_filter_visits', 
    default_args=default_args, 
    schedule_interval='@hourly',
    catchup=False  # To avoid running for past dates
) as dag:
    
    fetch_visits_task = SimpleHttpOperator(
        task_id='fetch_all_visits',
        method='GET',
        http_conn_id='openmrs_api_connection',  # Airflow connection to OpenMRS API
        endpoint='api/visit/visits',  # Adjust according to API specs
        headers={
            'Authorization': f'Basic {base64_auth_string}',
            'Cookie': f'JSESSIONID={session_cookie}'
        },
        response_filter=lambda response: response.text,  # Return the raw response text
        log_response=True
    )

    filter_visits_task = PythonOperator(
        task_id='filter_new_visits',
        python_callable=filter_visits,
        provide_context=True,
        op_args=['{{ task_instance.xcom_pull(task_ids="fetch_all_visits") }}']
    )

    fetch_visits_task >> filter_visits_task

# Uncomment the following line if you want to test the task manually
# airflow tasks test fetch_and_filter_visits filter_new_visits 2024-06-23