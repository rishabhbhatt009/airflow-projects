import os
from datetime import datetime,timedelta
from airflow import DAG
import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


def create_date_time_file(directory:str):
    if os.path.exists(directory):
        print("Folder Exists")
        file_name = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.txt'
        file_path = os.path.join(directory, file_name)
        with open(file_path, 'w') as file:
            print(f'File created at {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}')

        print(f'File created: {file_path}')
    else :
        print("Folder Doesn't Exist")

def branching_condition(directory:str):
    print(os.getcwd())
    if os.path.exists(directory):
        print("Folder Exists - Create File")
        return 'task3'
    else : 
        print("Folder Doesn't Exist - Create Folder")
        return 'task2'

default_args = {
    'owner':'Rishabh',
    'retries':2,
    'retry_delay':timedelta(seconds=10)
}

with DAG(
    dag_id='client7',
    description='demo dag',
    schedule_interval=timedelta(seconds=30),
    start_date=datetime(2023,11,17,0,0),
    max_active_runs=1,
    catchup=False 
) as dag : 
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo Hello World!'
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branching_condition,
        op_kwargs={'directory': './Test2'}
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='mkdir -p $AIRFLOW_HOME/Test2'
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=create_date_time_file,
        op_kwargs={'directory': './Test2'},
        trigger_rule='none_failed_min_one_success'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='echo Task Completed'
    )

    task1 >> branch_task 
    branch_task >> [task2,task3]
    task2 >> task3 
    task3 >> task4
    # task1 >> task2 >> task3 >> task4