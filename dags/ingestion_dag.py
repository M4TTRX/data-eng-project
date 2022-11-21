import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEATH_DATASET_ID = '5de8f397634f4164071119c5'
INGESTION_DATAPATH = 'dags/data/ingestion/'
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
    import json

    try:
        data = requests.get(GET_DEATH_DATASET_URL).json()
    except:
        print('An error occurred when pulling the death file list')
    json_object = json.dumps(data['resources'])
    with open(f'{INGESTION_DATAPATH}death_resources.json', 'w') as outfile:
        outfile.write(json_object)
    print('An error occurred when saving the list')

def pull_all_death_files(max_resource = 2):
    import json
    death_resources = json.load(open(f'{INGESTION_DATAPATH}death_resources.json', 'r'))
    import requests
    count = 0
    for resource in death_resources:
        count += 1
        if count > max_resource:
            print(f'Acquired the maximum of {max_resource} resources')
            break

        # pull the latest resource data
        response = requests.get(resource['latest'])
        if response.status_code == 200:
            with open(f'{INGESTION_DATAPATH}death_{resource["title"]}', 'w') as outfile:
                outfile.write(response.content.decode("utf-8"))
        else:
            print(f'Failed to get resource: {resource["title"]} at url {resource["latest"]}')

def _get_spreadsheet():
    return

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
    python_callable=_get_spreadsheet,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,  
)


get_death_resource_list = PythonOperator(
    task_id='get_death_resource_list',
    dag=ingestion_dag,  
    python_callable=pull_death_file_list,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False, 
)

get_death_resources = PythonOperator(
    task_id='get_death_resources',
    dag=ingestion_dag,     
    python_callable=pull_all_death_files,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False, 
)

end = DummyOperator(
    task_id='end',
    dag=ingestion_dag,   
    trigger_rule='all_success'
)



start >> [get_nuclear_json,get_death_resource_list,get_thermal_json]
get_nuclear_json >> get_nuclear_datas
get_death_resource_list >> get_death_resources
get_thermal_json >> get_thermal_datas
[get_nuclear_datas,get_death_resources,get_thermal_datas] >> end