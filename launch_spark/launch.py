import json

import requests

# Get the service address from config.json
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
        infra_service_address = config.get('infra_service_address', "https://dcluster-us-dev-a-dev.dv-api.com")
except (FileNotFoundError, json.JSONDecodeError):
    infra_service_address = "https://dcluster-us-dev-a-dev.dv-api.com"
max_wait_retries = 120
max_delete_retries = 3

cluster = {
    'serviceAddress': '',
    'externalURl': '',
    'id': '',
    'name': '',
    'tenant': '',
    'workers': '',
    'workerCpu': '',
    'workerMemory': '',
    'masterCpu': '',
    'masterMemory': '',
    'namespace': '',
    'status': '',
    'created': '',
    'deleted': '',
    'type': '',
    'launchNewNode': ''
}

headers = {
    'Content-Type': 'application/json',
}

cluster['tenant'] = 'zks'
cluster['type'] = 'spark'
cluster['workers'] = 10
cluster['masterCpu'] = 1
cluster['masterMemory'] = 4
cluster['workerCpu'] = 1
cluster['workerMemory'] = 7
cluster['launchNewNode'] = True


def launch_spark_cluster():
    request_body = json.dumps(cluster)

    response = requests.post(infra_service_address + '/cluster/v2/launch/spark/cluster',
                             headers=headers, data=request_body)

    if isinstance(response.json(), dict):
        for key, value in response.json().items():
            if isinstance(value, int):
                return value
    elif isinstance(response.json(), int):
        return response.json()

    return -1


if __name__ == '__main__':
    cluster_id = launch_spark_cluster()
    print("Cluster ID: " + str(cluster_id))
