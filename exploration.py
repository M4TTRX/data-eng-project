    
DEATH_DATASET_ID = '5de8f397634f4164071119c5'
GET_DEATH_DATASET_URL = f'https://www.data.gouv.fr/api/1/datasets/{DEATH_DATASET_ID}/'


def pull_nuclear_plants():
    import json
    response = json.load(open('dags/nuclear_plants.json', 'r'))
    import requests
    for resource in response['resources']:
        if resource['format'] == 'csv':
            csv_resource = requests.get(resource['latest'])
            if csv_resource.status_code == 200:
                with open(f'dags/data/ingestion/nuclear_{resource["last_modified"]}.csv', 'w') as outfile:
                    outfile.write(csv_resource.content.decode("utf-8"))
            else:
                print(f'Failed to extract nuclear plant data')
            return
    print('Could not file resource in csv format')

    #     count += 1
    #     if count > max_resource:
    #         print(f'Acquired the maximum of {max_resource} resources')
    #         break

    #     # pull the latest resource data
    #     response = requests.get(resource['latest'])
    #     if response.status_code == 200:
    #     else:
    #         print(f'Failed to get resource: {resource["title"]} at url {resource["latest"]}')

pull_nuclear_plants()