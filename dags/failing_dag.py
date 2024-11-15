from datetime import datetime, time, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define the function that will fail
def failing_task():
    raise ValueError("This task is designed to fail!")

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='failing_dag',
    default_args=default_args,
    schedule_interval='*/1 8-16 * * *',  # Runs every 1 minute between 8am and 5pm
    catchup=False,
) as dag:

    # Define the task
    task_fail = PythonOperator(
        task_id='task_fail',
        python_callable=failing_task,
    )

# Set the task in the DAG
task_fail