    
DEATH_DATASET_ID = '5de8f397634f4164071119c5'
GET_DEATH_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{DEATH_DATASET_ID}/'

import requests
import json

try:
    data = requests.get(GET_DEATH_DATASET_URL).json()
except:
    print("An error occured when pulling the death file list")
json_object = json.dumps(data['resources'])
with open("dags/data/death_resources.json", "w") as outfile:
    outfile.write(json_object)
print("An error occured when saving the list")