import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'

INGESTION_DATA_PATH = 'dags/data/ingestion/'
STAGING_DATA_PATH = 'dags/data/staging/'

# DAG definition
# ======================================
default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

staging_dag = DAG(
    dag_id='staging_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Python functions
# ======================================


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


# Operator definition
# ======================================
start = DummyOperator(
    task_id='start',
    dag=staging_dag,
)

end = DummyOperator(
    task_id='end',
    dag=staging_dag,
    trigger_rule='all_success'
)

import_death_clean_data = PythonOperator(
    task_id='load_data_from_ingestion',
    dag=staging_dag,
    python_callable=load_data_from_ingestion,
    op_kwargs={},
    depends_on_past=False,
)

cleanse_death_data = PythonOperator(
    task_id='clean_death_data',
    dag=staging_dag,
    python_callable=_cleanse_death_data,
    op_kwargs={},
    depends_on_past=False,
)
import_thermal_clean_data = PythonOperator(
    task_id='load_and_clean_thermal_datas',
    dag=staging_dag,
    python_callable=load_and_clean_thermal_datas,
    op_kwargs={},
    depends_on_past=False,
)


import_nuclear_clean_data = PythonOperator(
    task_id='load_and_clean_nuclear_datas',
    dag=staging_dag,
    python_callable=load_and_clean_nuclear_datas,
    op_kwargs={},
    depends_on_past=False,
)


start >> [import_nuclear_clean_data,
          import_thermal_clean_data, import_death_clean_data] >> end
