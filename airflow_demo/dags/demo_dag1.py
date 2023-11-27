import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def check_dir(dir_name): 
    print(os.getcwd())
    if os.path.exists(dir_name):
        print('Directory exists')
        return 'create_file'
    else: 
        print('Directory does not exist')
        return 'end' 

def create_date_time_file(dir_name):
    print(os.getcwd())
    if not os.path.exists(dir_name):
        print('Directory does not exist')
    else: 
        file_name = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.txt'
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, 'w') as file:
            print(f'File created at {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')
        print(f'File created: {file_path}')

default_args = {
    'owner':'Rishabh',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='sample_branching',
    default_args=default_args, 
    description='This is a sample dag',
    start_date=datetime(2023,11,15,19),
    schedule_interval=timedelta(seconds=30), # options: @daily / cron-string 
    catchup=False 
) as dag : 
    
    start = BashOperator(
        task_id='start',
        bash_command='echo Hello World!'
    )

    create_dir = BashOperator(
        task_id='create_dir',
        bash_command='mkdir -p /opt/airflow/outputs/Test && pwd'
    )

    check = BranchPythonOperator(
        task_id='check_dir',
        python_callable=check_dir,
        op_kwargs={
            'dir_name':'./outputs/Test'
        }
    )
    
    create_file = PythonOperator(
        task_id='create_file',
        python_callable=create_date_time_file,
        op_kwargs={
            'dir_name':'./outputs/Test'
        }
    )
    
    end = EmptyOperator(
        task_id='end'
    )

    start >> create_dir >> check
    check >> create_file >> end 
    check >> end