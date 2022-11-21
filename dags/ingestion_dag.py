import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEATH_DATASET_ID = '5de8f397634f4164071119c5'
GET_DEATH_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{DEATH_DATASET_ID}/'
# DAG definition

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

ingestion_dag = DAG(
    dag_id='ingestion_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Python functions
# ===================

def pull_death_file_list():
    import requests
    try:
        data = requests.get(GET_DEATH_DATASET_URL).json()
    except:
        print("An error occured when pulling the death file list")
        return
    data


# Operator definition
# ===================

start = DummyOperator(
    task_id='start',
    dag=ingestion_dag,
)

get_nuclear_json = BashOperator(
    task_id='get_nuclear_json',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/63587afc1e8e90e9ce487174/ --output /opt/airflow/dags/nuclear_plants.json",
)

get_nuclear_datas = PythonOperator(
    task_id='get_nuclear_datas',
    dag=ingestion_dag,   
    python_callable=_get_spreadsheet,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
) 

get_thermal_json = BashOperator(
    task_id='get_thermal_plants_json',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/63587afb1cc488641390f68e/ --output /opt/airflow/dags/thermal_plants.json",
)

get_thermal_datas = PythonOperator(
    task_id='get_thermal_datas',
    dag=ingestion_dag, 
    dag=ingestion_dag,   
    python_callable=_get_spreadsheet,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,  
)

get_death_json = BashOperator(
    task_id='get_deaths_json',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/ --output /opt/airflow/dags/deaths.json",
)

get_death_datas = PythonOperator(
    task_id='get_death_datas',
    dag=ingestion_dag,  
    dag=ingestion_dag,   
    python_callable=_get_spreadsheet,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False, 
)

end = DummyOperator(
    task_id='end',
    dag=ingestion_dag,   
)


start >> [get_nuclear_json,get_death_json,get_thermal_json]
get_nuclear_json >> get_nuclear_datas
get_death_json >> get_death_datas
get_thermal_json >> get_thermal_datas
[get_nuclear_datas,get_death_datas,get_thermal_datas] >> end