from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import requests

import sys
import os

# Add the parent directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the actual implementation of get_fresh_token from your scripts.token_retrieval module
from scripts.token_retrieval import get_fresh_token

default_args = {
    'owner': 'munyah',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

def retrieve_token():
    try:
        token = get_fresh_token()  
        print(f"Retrieved Token: {token}")
        return token
    except Exception as e:
        print(f"Error retrieving token: {e}")
        raise

def print_headers(token):
    if not token:
        print("Token not found")
    else:
        headers = {"Authorization": f"Bearer {token}"}
        print(f"Headers: {headers}")

def call_api(token):
    print(f"Token before API call: {token}")  
    
    # Get the connection details that you set in airflow environment
    conn = BaseHook.get_connection('eezimeds_patients')
    endpoint = '/mongo/patient'  
    url = f"{conn.host}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.get(url, headers=headers)
        print(f"Response status code: {response.status_code}")
        response_data = response.json()
        print(f"Response data: {response_data}")
        
        # Handle token expiration and refresh if needed
        if response.status_code == 401: 
            print("Token expired. Refreshing...")
            token = get_fresh_token()  # Refresh the token
            headers["Authorization"] = f"Bearer {token}"  
            response = requests.get(url, headers=headers)  # Make the API call again
            response_data = response.json()
            print(f"Response after refresh: {response_data}")
            
        if response.status_code == 200:
            return filter_patients(response_data)
        else:
            print(f"Failed to retrieve patient. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error during API call: {e}")
        return None

def filter_patients(patient_data):
    # Calculate the date range for the previous day
    yesterday_start = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Filter patients created within the previous day
    filtered_patients = [
        patient for patient in patient_data
        if 'createdAt' in patient and yesterday_start <= datetime.fromisoformat(patient['createdAt']) <= yesterday_end
    ]
    
    if not filtered_patients:
        print("No patients registered a day ago")
        return []
    else:
        print(f"Patient data: {filtered_patients}")
        return filtered_patients

def handle_filtered_patients(**context):
    filtered_patients = context['task_instance'].xcom_pull(task_ids='retrieve_patients')
    if not filtered_patients:
        print("No patients registered a day ago")
    else:
        print(f"Filtered Patient data: {filtered_patients}")

with DAG(
    'patients_api_dag',
    default_args=default_args,
    description='A DAG to retrieve patient data from the eezimeds API',
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:

    retrieve_token_task = PythonOperator(
        task_id='retrieve_token',
        python_callable=retrieve_token,
    )

    debug_headers_task = PythonOperator(
        task_id='debug_headers',
        python_callable=print_headers,
        op_args=[retrieve_token_task.output],  
    )

    retrieve_patient_task = PythonOperator(
        task_id='retrieve_patients',
        python_callable=call_api,
        op_args=[retrieve_token_task.output],
    )

    handle_filtered_patients_task = PythonOperator(
        task_id='handle_filtered_patients',
        python_callable=handle_filtered_patients,
        provide_context=True,
    )

    retrieve_token_task >> debug_headers_task >> retrieve_patient_task >> handle_filtered_patients_task
