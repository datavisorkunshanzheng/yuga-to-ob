import json
import subprocess
import time

import boto3

config = {}

task_list = {}

s3 = boto3.client('s3')


def s3_list_objects(bucket, prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as e:
        print(f"Error listing objects in S3: {e}")
        return []

    if 'Contents' in response:
        return [item['Key'] for item in response['Contents']]
    return []


def generate_task_list():
    if len(config['table_list']) != 0:
        for table in config['table_list']:
            tenant = table.split('.')[0]
            table_name = table.split('.')[1]

            for file in s3_list_objects(config['input_s3'],
                                        f"{config['input_s3_prefix']}/{tenant}/{table_name}/"):
                if file.endswith('.orc'):
                    task_list[f"/{file}"] = 0
    else:
        for tenant in s3_list_objects(config['input_s3'], config['input_s3_prefix']):
            for table in s3_list_objects(config['input_s3'], f"{tenant}"):
                for file in s3_list_objects(config['input_s3'], f"{table}"):
                    if file.endswith('.orc'):
                        task_list[f"/{file}"] = 0


def load_to_db(task_path):
    tenant = task_path.split('/')[-3]
    table = task_path.split('/')[-2]
    command = (
        f"{config['script_path']} -h \'{config['sql_host']}\' -P \'{config['sql_port']}\' -u \'{config['sql_user']}\' "
        f"-p \'{config['sql_password']}\' -t \'{config['sql_tenant']}\' -D \'{tenant}\' --table \'{table}\' --orc "
        f"-f \'s3://{config['input_s3']}{task_path}?access-key={config['s3_access_key']}"
        f"&secret-key={config['s3_secret_key']}&region=us-east-1\' "
        f"--sys-password \'{config['sql_password']}\' --direct --parallel={config['parallelization_level']} "
        f"--rpc-port={config['rpc_port']} --replace-data --slow 0.95 --pause 0.99")

    try:
        subprocess.run(command, shell=True, check=True)
        del task_list[task_path]
    except Exception as e:
        print(f"Error loading {task_path} to DB: {e}")
        task_list[task_path] += 1
        if task_list[task_path] > config['max_retry']:
            print(f"Task {task_path} has reached maximum retries")
            del task_list[task_path]


if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)

    generate_task_list()

    start_time = time.time()

    while True:
        if len(task_list) == 0:
            break

        load_to_db(list(task_list.keys())[0])

    print(f"Total time taken: {time.time() - start_time} seconds")
