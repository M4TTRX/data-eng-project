import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
    import json

    try:
        data = requests.get(GET_DEATH_DATASET_URL).json()
    except:
        print("An error occurred when pulling the death file list")
    json_object = json.dumps(data['resources'])
    with open("dags/data/death_resources.json", "w") as outfile:
        outfile.write(json_object)
    print("An error occurred when saving the list")

def pull_all_death_files(max_resource = 2):
    import json
    death_resources = json.load(open('dags/data/ingestion/death_resources.json', 'r'))
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
            with open(f'dags/data/ingestion/death_{resource["title"]}', 'w') as outfile:
                outfile.write(response.content.decode("utf-8"))
        else:
            print(f'Failed to get resource: {resource["title"]} at url {resource["latest"]}')

# Operator definition
# ===================
# Downloading a file from an API/endpoint?

task_one = BashOperator(
    task_id='get_nuclear_datas',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/63587afc1e8e90e9ce487174/ --output /opt/airflow/dags/nuclear_plants.json",
)

# oh noes :( it's xlsx... let's make it a csv.

task_two = BashOperator(
    task_id='get_thermal_plants_datas',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/63587afb1cc488641390f68e/ --output /opt/airflow/dags/thermal_plants.json",
)

task_three = BashOperator(
    task_id='get_deaths_datas',
    dag=ingestion_dag,
    bash_command="curl https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/ --output /opt/airflow/dags/deaths.json",
)


task_three >> task_one >> task_two 
