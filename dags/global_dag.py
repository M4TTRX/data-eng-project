import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

DEATH_DATASET_ID = '5de8f397634f4164071119c5'
THERMAL_DATASET_ID = '63587afb1cc488641390f68e'
NUCLEAR_DATASET_ID = '63587afc1e8e90e9ce487174'
INGESTION_DATA_PATH = 'dags/data/ingestion/'
GET_DEATH_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{DEATH_DATASET_ID}/'
GET_THERMAL_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{THERMAL_DATASET_ID}/'
GET_NUCLEAR_DATAET_URL = f'https://www.data.gouv.fr/api/1/datasets/{NUCLEAR_DATASET_ID}/'
# DAG definition

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

global_dag = DAG(
    dag_id='global_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Python functions
# ===================


def pull_thermal_plants_data():
    import json
    thermal_resources = json.load(
        open(f'{INGESTION_DATA_PATH}thermal_plants.json', 'r'))
    import requests
    for resource in thermal_resources['resources']:
        if resource['format'] == 'csv':
            response = requests.get(resource['latest'])
            if response.status_code == 200:
                with open(f'dags/data/ingestion/thermal_plants_{resource["title"]}.csv', 'w') as outfile:
                    outfile.write(response.content.decode("utf-8"))
            else:
                print(
                    f'Failed to get thermal plants resource')
    print('Could not file resource in csv format')

def pull_death_file_list():
    import requests
    try:
        data = requests.get(GET_DEATH_DATASET_URL).json()
    except:
        print('An error occurred when pulling the death file list')
    import json
    json_object = json.dumps(data['resources'])
    with open(f'{INGESTION_DATA_PATH}death_resources.json', 'w') as outfile:
        outfile.write(json_object)
    print('An error occurred when saving the death list list')


def pull_all_death_files(max_resource=5):
    import json
    death_resources = json.load(
        open(f'{INGESTION_DATA_PATH}death_resources.json', 'r'))
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
            with open(f'{INGESTION_DATA_PATH}death_{resource["title"]}', 'w') as outfile:
                outfile.write(response.content.decode("utf-8"))
        else:
            print(
                f'Failed to get resource: {resource["title"]} at url {resource["latest"]}')


def pull_nuclear_plants():
    import json
    response = json.load(open(f'{INGESTION_DATA_PATH}nuclear_plants.json', 'r'))
    import requests
    for resource in response['resources']:
        if resource['format'] == 'csv':
            csv_resource = requests.get(resource['latest'])
            if csv_resource.status_code == 200:
                with open(f'{INGESTION_DATA_PATH}nuclear_{resource["last_modified"]}.csv', 'w') as outfile:
                    outfile.write(csv_resource.content.decode("utf-8"))
            else:
                print(f'Failed to extract nuclear plant data')
            return
    print('Could not file resource in csv format')

# Operator definition
# ===================


with TaskGroup("ingestion_pipeline","data ingestion step",dag=global_dag) as ingestion_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    get_nuclear_json = BashOperator(
        task_id='get_nuclear_json',
        dag=global_dag,
        bash_command=f'curl {GET_NUCLEAR_DATAET_URL} --output /opt/airflow/{INGESTION_DATA_PATH}/nuclear_plants.json',
    )

    get_nuclear_data = PythonOperator(
        task_id='get_nuclear_data',
        dag=global_dag,
        python_callable=pull_nuclear_plants,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    get_thermal_json = BashOperator(
        task_id='get_thermal_plants_json',
        dag=global_dag,
        bash_command=f'curl {GET_THERMAL_DATASET_URL} --output /opt/airflow/{INGESTION_DATA_PATH}/thermal_plants.json',
    )

    get_thermal_data = PythonOperator(
        task_id='get_thermal_data',
        dag=global_dag,
        python_callable=pull_thermal_plants_data,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )


    get_death_resource_list = PythonOperator(
        task_id='get_death_resource_list',
        dag=global_dag,
        python_callable=pull_death_file_list,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    get_death_resources = PythonOperator(
        task_id='get_death_resources',
        dag=global_dag,
        python_callable=pull_all_death_files,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [get_nuclear_json, get_death_resource_list, get_thermal_json]
    get_nuclear_json >> get_nuclear_data
    get_death_resource_list >> get_death_resources
    get_thermal_json >> get_thermal_data
    [get_nuclear_data, get_death_resources, get_thermal_data] >> end

start_global = DummyOperator(
    task_id='start_global',
    dag=global_dag,
    trigger_rule='all_success'
)

end_global = DummyOperator(
    task_id='end_global',
    dag=global_dag,
    trigger_rule='all_success'
)

start_global >> ingestion_pipeline >> end_global
