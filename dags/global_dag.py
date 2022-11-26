import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

DEATH_DATASET_ID = '5de8f397634f4164071119c5'
THERMAL_DATASET_ID = '63587afb1cc488641390f68e'
NUCLEAR_DATASET_ID = '63587afc1e8e90e9ce487174'
CITY_GEO_DATA_ID = 'dbe8a621-a9c4-4bc3-9cae-be1699c5ff25'
INGESTION_DATA_PATH = 'dags/data/ingestion/'
DATA_GOUV_BASE_URL = 'https://www.data.gouv.fr/api/1/datasets/'
GET_DEATH_DATASET_URL = DATA_GOUV_BASE_URL + DEATH_DATASET_ID
GET_THERMAL_DATASET_URL = 'https://www.data.gouv.fr/api/1/datasets/63587afb1cc488641390f68e/'
GET_NUCLEAR_DATAET_URL = 'https://www.data.gouv.fr/api/1/datasets/63587afc1e8e90e9ce487174/'
CITY_GEO_DATASET_URL = 'https://static.data.gouv.fr/resources/communes-de-france-base-des-codes-postaux/20200309-131459/communes-departement-region.csv'
STAGING_DATA_PATH = 'dags/data/staging/'


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
def load_and_clean_thermal_datas():
    data_1 = pd.read_csv(
        './dags/data/ingestion/thermal_plants_.csv', error_bad_lines=False, sep=';')
    data_1 = data_1.drop(columns={'perimetre_spatial', 'filiere', 'combustible',
                         'reserve_secondaire_maximale', 'sous_filiere', 'unite'})
    data_1 = data_1.rename(columns={'centrale': 'plant', 'point_gps_wsg84': 'position', 'commune': 'city',
                           'date_de_mise_en_service_industrielle': 'start_date', 'puissance_installee': 'power (MW)'})
    data_1.to_csv('./dags/data/staging/thermal_plants_clean.csv')


def load_and_clean_nuclear_datas():
    data_1 = pd.read_csv('./dags/data/ingestion/nuclear.csv',
                         error_bad_lines=False, sep=';')
    data_1 = data_1.drop(columns={'reserve_secondaire_maximale', 'puissance_minimum_de_conception',
                         'sub_sector', 'perimetre_spatial', 'combustible', 'filiere', 'unite'})
    data_1 = data_1.rename(columns={'centrale': 'plant', 'sous_filiere': 'sub_sector', 'contrat_programme': 'contract', 'point_gps_wsg84': 'position',
                           'commune': 'city', 'date_de_mise_en_service_industrielle': 'start_date', 'puissance_installee': 'power (MW)'})
    data_1.to_csv('./dags/data/staging/nuclear_clean_datas.csv')


def get_redis_client():
    import redis
    return redis.Redis(host='redis', port=6379, db=0)


def load_data_from_ingestion():
    import os
    death_files = [os.path.join(root, name)
                   for root, dirs, files in os.walk(INGESTION_DATA_PATH)
                   for name in files
                   if name.startswith(("death_"))]

    # pull imported files from redis
    r = get_redis_client()

    imported_deaths = [file_path.decode(
        "utf-8") for file_path in r.lrange('imported_death_files', 0, -1)]

    # load the ones that have not been loaded yet
    files_to_load = [
        file_path for file_path in death_files if file_path not in imported_deaths]
    print(f'{len(imported_deaths)}/{len(death_files)} files already imported. Importing {len(files_to_load)} files: {str(files_to_load)}')
    import json
    import hashlib
    for file_path in files_to_load:
        file = open(file_path, 'r')
        for line in file.readlines():
            # hash the person's name, we dont need it and it increases privacy
            dead_person = {
                'id': hashlib.sha1(line[:80].encode()).hexdigest(),
                'location': line[162:167].strip(),
                'date': line[154:162].strip()
            }
            r.lpush('death_raw', json.dumps(dead_person))

        r.lpush('imported_death_files', file_path)
        print(f'{file} successfully imported')
    # preliminary processing and store
    return


def _cleanse_death_data():
    import json
    r = get_redis_client()
    # load imports from redis
    result = r.lrange('death_raw', 0, -1)
    print(result)
    data = [json.loads(element.decode("utf-8"))
            for element in result]
    print(data)
    import pandas as pd
    df = pd.DataFrame(data)
    print("Successfully loaded death data")
    city_info = pd.read_csv(f'{INGESTION_DATA_PATH}city_geo_loc.csv')
    city_info = city_info.rename(columns={'code_commune_INSEE': 'location'})
    merged_death_data = df.merge(
        city_info[['location', 'latitude', 'longitude']], on='location', how='left')

    # drop rows where the location led to no known cities, this occures mostly with INSEE codes that are over 99000
    merged_death_data = merged_death_data[merged_death_data['latitude'].notna(
    )]
    merged_death_data = merged_death_data[merged_death_data['longitude'].notna(
    )]

    merged_death_data.to_csv(f'{STAGING_DATA_PATH}/clean_death_data.csv')
    print("Successfully location to geopoints")

def pull_thermal_plants_data():
    import json
    thermal_resources = json.load(
        open(f'{INGESTION_DATA_PATH}thermal_plants.json', 'r'))
    import requests
    for resource in thermal_resources['resources']:
        if resource['format'] == 'csv':
            response = requests.get(resource['latest'])
            if response.status_code == 200:
                with open(f'{INGESTION_DATA_PATH}thermal_plants_.csv', 'w') as outfile:
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
                with open(f'{INGESTION_DATA_PATH}nuclear.csv', 'w') as outfile:
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

    get_city_code_geo = BashOperator(
        task_id='get_city_code_geo',
        dag=global_dag,
        bash_command=f'curl {CITY_GEO_DATASET_URL} --output /opt/airflow/{INGESTION_DATA_PATH}/city_geo_loc.csv',
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    start >> [get_city_code_geo, get_nuclear_json, get_death_resource_list, get_thermal_json]
    get_nuclear_json >> get_nuclear_data
    get_death_resource_list >> get_death_resources
    get_thermal_json >> get_thermal_data
    [get_nuclear_data, get_death_resources, get_thermal_data, get_city_code_geo] >> end

with TaskGroup("staging_pipeline","data staging step",dag=global_dag) as staging_pipeline:
    start = DummyOperator(
        task_id='start',
        dag=global_dag,
    )

    end = DummyOperator(
        task_id='end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    import_death_clean_data = PythonOperator(
        task_id='import_death_clean_data',
        dag=global_dag,
        python_callable=load_data_from_ingestion,
        op_kwargs={},
        depends_on_past=False,
    )

    cleanse_death_data = PythonOperator(
        task_id='cleanse_death_data',
        dag=global_dag,
        python_callable=_cleanse_death_data,
        op_kwargs={},
        depends_on_past=False,
    )
    import_thermal_clean_data = PythonOperator(
        task_id='import_thermal_clean_data',
        dag=global_dag,
        python_callable=load_and_clean_thermal_datas,
        op_kwargs={},
        depends_on_past=False,
    )


    import_nuclear_clean_data = PythonOperator(
        task_id='import_nuclear_clean_data',
        dag=global_dag,
        python_callable=load_and_clean_nuclear_datas,
        op_kwargs={},
        depends_on_past=False,
    )

    start >> [import_nuclear_clean_data,import_thermal_clean_data, import_death_clean_data]
    import_death_clean_data >> cleanse_death_data
    [import_nuclear_clean_data,import_thermal_clean_data, cleanse_death_data] >> end

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

start_global >> ingestion_pipeline >> staging_pipeline >> end_global
