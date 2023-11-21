import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def create_date_time_file():
    directory = 'Test'
    if not os.path.exists(directory):
        print('Python Op creating dir')
        os.makedirs(directory)

    file_name = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.txt'
    file_path = os.path.join(directory, file_name)
    with open(file_path, 'w') as file:
        print(f'File created at {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')

    print(f'File created: {file_path}')

default_args = {
    'owner':'Rishabh',
    'retries':5,
    'retry_delay':timedelta(seconds=15)
}

with DAG(
    dag_id='client1',
    description='demo dag',
    start_date=datetime(2023,11,15,19),
    schedule_interval=timedelta(minutes=1),
    catchup=False 
) as dag : 
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo Hello World!'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='mkdir -p Test'
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=create_date_time_file
    )

    task1 >> task2 >> task3