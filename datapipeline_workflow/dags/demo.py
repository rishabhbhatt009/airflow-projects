import os 
import sys
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Add AIRFLOW_HOME to sys.path
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
if AIRFLOW_HOME and AIRFLOW_HOME not in sys.path:
    sys.path.insert(0, AIRFLOW_HOME)
    print(f'Added {AIRFLOW_HOME} to PATH')

from pipeline_script import utils

##############################################################################################
# Pipeline Setup
##############################################################################################
config = utils.get_config('config/pipeline_config.json')
connection = utils.connect_db(
    host=config['hostname'], 
    username=config['username'], 
    password=config['password']
    )

##############################################################################################
# Creating DAG1
##############################################################################################

default_args = {
    'owner':'Rishabh',
    'retries':2,
    'retry_delay':timedelta(seconds=10)
}

dag1 = DAG(
    dag_id='    ',
    default_args=default_args,
    description='demo dag',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2023,11,17,0,0),
    max_active_runs=1,
    catchup=False
) 

##############################################################################################
# Creating Tasks
##############################################################################################

test_connection = PythonOperator(
    task_id='test_db_connection',
    python_callable=utils.test_aurora_connection,
    op_kwargs={
        'host' : config['hostname'],
        'username' : config['username'],
        'password' : config['password'],
        },
    dag = dag1
    )

create_op_dir = BashOperator(
    task_id='create_output_dir',
    bash_command='mkdir -p $AIRFLOW_HOME/outputs'
)

gen_schema = PythonOperator(
    task_id='generate_server_schema',
    python_callable=utils.generate_server_schema,
    op_kwargs={
        'connection': connection, 
        'path': 'outputs/'
        },
    dag=dag1,
)

get_num_accs = PythonOperator(
    task_id='get_num_accounts',
    python_callable=utils.run_sql_script,
    op_kwargs={
        'connection': connection, 
        'script': 'sql_scripts/num_accounts.sql'
        },
    dag=dag1,
)

get_num_teams = PythonOperator(
    task_id='get_num_teams',
    python_callable=utils.run_sql_script,
    op_kwargs={
        'connection': connection, 
        'script': 'sql_scripts/num_teams.sql'
        },
    dag=dag1,
)

disconnect = PythonOperator(
    task_id='disconnect',
    python_callable=utils.disconnect,
    op_kwargs={
        'connection': connection
        },
    dag=dag1,
)

##############################################################################################
# Task dependencies
##############################################################################################

test_connection >> create_op_dir >> [gen_schema, get_num_accs, get_num_teams] >> disconnect


##############################################################################################