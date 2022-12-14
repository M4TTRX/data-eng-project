from cmath import nan
import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

DEATH_INSERTION_QUERIES = 'death_insert.sql'
PLANT_INSERTION_QUERIES = 'plant_insert.sql'
DEATH_INSERTION_QUERIES_PATH = f'dags/sql/tmp/{DEATH_INSERTION_QUERIES}'
PLANT_INSERTION_QUERIES_PATH = f'dags/sql/tmp/{PLANT_INSERTION_QUERIES}'


# DAG definition

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
}

global_dag = DAG(
    dag_id='global_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

# Python functions
# ===================
def _import_thermal_clean_data():
    data_1 = pd.read_csv(
        './dags/data/ingestion/thermal_plants_.csv', error_bad_lines=False, sep=';')
    data_1 = data_1.drop(columns={'perimetre_spatial', 'filiere', 'combustible',
                         'reserve_secondaire_maximale', 'sous_filiere', 'unite'})
    data_1 = data_1.rename(columns={'centrale': 'plant', 'point_gps_wsg84': 'position', 'commune': 'city',
                           'date_de_mise_en_service_industrielle': 'start_date', 'puissance_installee': 'power (MW)'})
    data_1.to_csv('./dags/data/staging/thermal_plants_clean.csv')


def _import_nuclear_clean_data():
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


def _load_data_from_ingestion():
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
                'death_date': line[154:162].strip(),
                'birth_date': line[81:89].strip()
            }
            r.lpush('death_raw', json.dumps(dead_person))

        r.lpush('imported_death_files', file_path)
        print(f'{file} successfully imported')
    # preliminary processing and store
    return

def to_postgres_date(raw_date : str):
    try:
        return str(datetime.datetime.strptime(raw_date, '%Y%m%d'))[:10]
    except:
        try:
            return str(datetime.datetime.strptime(raw_date, '%Y-%m-%d'))[:10]
        except:
            return None

def _cleanse_death_data():
    import json
    r = get_redis_client()
    # load imports from redis
    result = r.lrange('death_raw', 0, -1)
    death_data = [json.loads(element.decode("utf-8"))
            for element in result]
    import pandas as pd
    print("Successfully loaded death data")
    insee_code_to_geo = {loc['code_commune_INSEE']: (loc['latitude'], loc['longitude']) for _, loc in pd.read_csv(f'{INGESTION_DATA_PATH}city_geo_loc.csv').iterrows()}
    query = ''
    count = 0
    for death in death_data:
        if death['location'] in insee_code_to_geo:
            location = insee_code_to_geo[death['location']]

            # convert date to postgres date format
            birth_date = to_postgres_date(death['birth_date'])
            death_date = to_postgres_date(death['death_date'])

            # drop invalid data
            import math
            if math.isnan(location[0]) or math.isnan(location[1]) or birth_date is None or death_date is None:
                continue
            query += f"INSERT INTO deaths VALUES ('{death['id']}', '{birth_date}', '{death_date}', '{location[0]}', '{location[1]}') ON CONFLICT DO NOTHING;\n"

    # Save sql querys

    with open(DEATH_INSERTION_QUERIES_PATH, "w") as f : f.write(query)
    print("Created SQL query")

def _death_emptiness_check():
        with open(DEATH_INSERTION_QUERIES_PATH, "r") as f : 
            # check if file is empty
            file_content = f.read()
            # check if file content is empty
            if len(file_content) == 0:
                return staging_end.task_id
            else:
                return store_deaths_in_postgres.task_id

def _clean_tmp_death_files():
    r = get_redis_client()
    result = r.lrange('death_raw', 0, -1)
    for _ in result : r.lpop('death_raw')
    import os
    if (os.path.exists(DEATH_INSERTION_QUERIES_PATH)):
        os.remove(DEATH_INSERTION_QUERIES_PATH)



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

def _create_plant_persist_sql_query():
    df_thermal = pd.read_csv('dags/data/staging/thermal_plants_clean.csv')
    # drop duplicate values of in the plant column of df_thermal
    df_thermal.drop_duplicates(subset=['plant'], inplace=True)
    
    df_nuclear = pd.read_csv('dags/data/staging/nuclear_clean_datas.csv')
    # drop duplicate values of in the plant column of df_nuclear
    df_nuclear.drop_duplicates(subset=['plant'], inplace=True)
    
    
    query = ''
    import hashlib
    for _, plant in df_thermal.iterrows():
        id = hashlib.sha1(str(plant).encode()).hexdigest()
        start_date = to_postgres_date(plant['start_date'])
        print('startdate '+ start_date)
        if start_date is None:
            continue
        position = plant['position'].split(',')
        query += f"INSERT INTO power_plants VALUES ('{id}', '{plant['plant']}', 'THERMAL', '{plant['fuel']}', '{start_date}', '{plant['power (MW)']}', '{position[0]}', '{position[1]}') ON CONFLICT DO NOTHING;\n"
    for _, plant in df_nuclear.iterrows():
        id = hashlib.sha1(str(plant).encode()).hexdigest()
        start_date = to_postgres_date(plant['start_date'])
        if start_date is None:
            continue
        position = plant['position'].split(',')
        query += f"INSERT INTO power_plants VALUES ('{id}', '{plant['plant']}', 'NUCLEAR', '{plant['fuel']}', '{start_date}', '{plant['power (MW)']}', '{position[0]}', '{position[1]}') ON CONFLICT DO NOTHING;\n"
    
    # Save sql querys
    import os
    if (os.path.exists(PLANT_INSERTION_QUERIES_PATH)):
        os.remove(PLANT_INSERTION_QUERIES_PATH)
    with open(PLANT_INSERTION_QUERIES_PATH, "w") as f:
        f.write(query)
    print("Created SQL query")

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

    staging_end = DummyOperator(
        task_id='staging_end',
        dag=global_dag,
        trigger_rule='all_success'
    )

    # Postgres operators

    create_death_table = PostgresOperator(
        task_id='create_death_table',
        dag=global_dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_death_table.sql',
        trigger_rule='none_failed',
        autocommit=True,
    )

    create_power_plants_table = PostgresOperator(
        task_id='create_power_plants_table',
        dag=global_dag,
        postgres_conn_id='postgres_default',
        sql='sql/create_power_plant_table.sql',
        trigger_rule='none_failed',
        autocommit=True,
    )

    store_deaths_in_postgres = PostgresOperator(
        task_id='store_deaths_in_postgres',
        dag=global_dag,
        postgres_conn_id='postgres_default',
        sql=f'sql/tmp/{DEATH_INSERTION_QUERIES}',
        trigger_rule='none_failed',
        autocommit=True,
    )

    store_plants_in_postgres = PostgresOperator(
        task_id='store_plants_in_postgres',
        dag=global_dag,
        postgres_conn_id='postgres_default',
        sql=f'sql/tmp/{PLANT_INSERTION_QUERIES}',
        trigger_rule='none_failed',
        autocommit=True,
    )

    # Python operators

    load_data_from_ingestion = PythonOperator(
        task_id='load_data_from_ingestion',
        dag=global_dag,
        python_callable=_load_data_from_ingestion,
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

    clean_tmp_death_files = PythonOperator(
        task_id='clean_tmp_death_files',
        dag=global_dag,
        python_callable=_clean_tmp_death_files,
        op_kwargs={},
        depends_on_past=False,
    )

    import_thermal_clean_data = PythonOperator(
        task_id='import_thermal_clean_data',
        dag=global_dag,
        python_callable=_import_thermal_clean_data,
        op_kwargs={},
        depends_on_past=False,
    )

    import_nuclear_clean_data = PythonOperator(
        task_id='import_nuclear_clean_data',
        dag=global_dag,
        python_callable=_import_nuclear_clean_data,
        op_kwargs={},
        depends_on_past=False,
    )

    create_plant_persist_sql_query = PythonOperator(
        task_id='create_plant_persist_sql_query',
        dag=global_dag,
        python_callable=_create_plant_persist_sql_query,
        op_kwargs={},
        trigger_rule='all_success',
        depends_on_past=False,
    )

    # Python Branch Operators

    death_emptiness_check = BranchPythonOperator(
        task_id='death_emptiness_check',
        dag=global_dag,
        python_callable=_death_emptiness_check,
        op_kwargs={
            'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
    "output_folder": "/opt/airflow/dags"
        },
        trigger_rule='all_success',
    )

    start >> [import_nuclear_clean_data, import_thermal_clean_data,create_power_plants_table, create_death_table]
    create_death_table >> load_data_from_ingestion >> cleanse_death_data >> death_emptiness_check
    death_emptiness_check >> [staging_end, store_deaths_in_postgres]
    store_deaths_in_postgres >> clean_tmp_death_files
    import_nuclear_clean_data >> import_thermal_clean_data >> create_power_plants_table >> create_plant_persist_sql_query >> store_plants_in_postgres
    [clean_tmp_death_files, death_emptiness_check,store_plants_in_postgres] >> staging_end

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
