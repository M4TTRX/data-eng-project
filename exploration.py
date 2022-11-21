    
DEATH_DATASET_ID = '5de8f397634f4164071119c5'
GET_DEATH_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{DEATH_DATASET_ID}/'

def pull_death_file_list():
    import requests
    import json

    try:
        data = requests.get(GET_DEATH_DATASET_URL).json()
    except:
        print('An error occured when pulling the death file list')
    json_object = json.dumps(data['resources'])
    with open('dags/data/ingestion/death_resources.json', 'w') as outfile:
        outfile.write(json_object)
    print('An error occured when saving the list')

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

pull_all_death_files()