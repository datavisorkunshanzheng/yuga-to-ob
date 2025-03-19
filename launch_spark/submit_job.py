#!/usr/bin/env python3
import json
import os
import requests
import subprocess
import sys
import time
import argparse

# Default configuration
DEFAULT_INFRA_SERVICE_ADDRESS = "https://dcluster-us-east-b-preprod.dv-api.com"
POLL_INTERVAL = 10  # seconds
MAX_RETRIES = 120

def parse_arguments():
    parser = argparse.ArgumentParser(description='Submit a Spark job to a cluster')
    parser.add_argument('--config', type=str, required=True,
                        help='Path to the JSON configuration file')
    parser.add_argument('--cluster_id', type=int, required=False,
                        help='Cluster ID returned from launch.py (overrides config file)')
    parser.add_argument('--destroy-cluster', action='store_true',
                        help='Destroy the cluster after job completion (overrides config)')
    parser.add_argument('--no-destroy-cluster', action='store_true',
                        help='Do not destroy the cluster after job completion (overrides config)')
    parser.add_argument('--local-mode', action='store_true',
                        help='Run in local mode - print curl commands instead of executing them')
    
    return parser.parse_args()

def load_config(config_path):
    """Load the configuration from a JSON file."""
    try:
        # Check if config file exists
        if not os.path.exists(config_path):
            print(f"Error: Config file not found at {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ['cassandra_host', 'cassandra_port', 'output_path', 'infra_service_address']
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            print(f"Error: Missing required fields in config: {', '.join(missing_fields)}")
            sys.exit(1)
            
        # Convert relative paths to absolute paths
        config_dir = os.path.dirname(os.path.abspath(config_path))
        if 'jar_path' in config and not os.path.isabs(config['jar_path']):
            config['jar_path'] = os.path.normpath(os.path.join(config_dir, config['jar_path']))
            
        # Don't modify S3 paths that start with s3:// or s3a://
        if 'output_path' in config and not (config['output_path'].startswith('s3://') or config['output_path'].startswith('s3a://')) and not os.path.isabs(config['output_path']):
            config['output_path'] = os.path.normpath(os.path.join(config_dir, config['output_path']))
            
        return config
    except json.JSONDecodeError as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config file: {e}")
        sys.exit(1)

def get_cluster_info(cluster_id, infra_service_address=None):
    """Get information about the cluster from the infrastructure service."""
    # Use provided infra_service_address or default
    service_address = infra_service_address or DEFAULT_INFRA_SERVICE_ADDRESS
    
    headers = {'Content-Type': 'application/json'}
    response = requests.get(f"{service_address}/cluster/status/spark/cluster/{cluster_id}", 
                           headers=headers)
    # Response example:
    # {
    #    "clusterIps": [
    #        "http://spark-master.spark-pre-zks-96.svc.cluster.local:8998"
    #    ],
    #    "externalURl": "",
    #    "launchNewNode": true,
    #    "serviceAddress": "http://spark-master.spark-pre-zks-96.svc.cluster.local:8998",
    #    "useOnDemandMaster": true,
    #    "interrupted": false,
    #    "type": "spark",
    #    "workerMemory": "7",
    #    "masterMemory": "4",
    #    "name": "spark-pre-zks-96",
    #    "namespace": "spark-pre-zks-96",
    #    "workerCpu": "1",
    #    "id": 96,
    #    "workers": 20,
    #    "tenant": "zks",
    #    "masterCpu": "1",
    #    "status": "STARTING"
    # }
    
    if response.status_code != 200:
        print(f"Error getting cluster info: {response.status_code} - {response.text}")
        sys.exit(1)
        
    return response.json()

def wait_for_cluster_ready(cluster_id, infra_service_address=None):
    """Wait until the cluster is in 'RUNNING' state."""
    print(f"Waiting for cluster {cluster_id} to be ready...")
    
    for attempt in range(MAX_RETRIES):
        cluster_info = get_cluster_info(cluster_id, infra_service_address)
        status = cluster_info.get('status')
        
        if status == 'RUNNING':
            print(f"Cluster {cluster_id} is now running!")
            return cluster_info
        
        print(f"Cluster status: {status}. Waiting {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)
    
    print(f"Timeout waiting for cluster to be ready after {MAX_RETRIES} attempts.")
    sys.exit(1)

def submit_spark_job(cluster_info, config):
    """Submit the Spark job to the cluster using the infra service API instead of direct connection."""
    # Extract service address from cluster info
    service_address = cluster_info.get('serviceAddress')
    
    if not service_address:
        print("Error: 'serviceAddress' not found in cluster_info")
        sys.exit(1)
    
    # Prepare the batches endpoint for job submission
    infra_endpoint = "http://172.27.34.184:31148/batches"
    print(f"Using infra service endpoint: {infra_endpoint}")
    
    # Get spark configuration from config or use defaults
    spark_config = config.get('spark_config', {})
    driver_memory = spark_config.get('driver_memory', '4g')
    executor_memory = spark_config.get('executor_memory', '7g')
    driver_cores = spark_config.get('driver_cores', 2)
    executor_cores = spark_config.get('executor_cores', 1)
    
    # Check if jar_path exists in config
    if 'jar_path' not in config and 's3_jar_path' not in config:
        print("Error: Neither 'jar_path' nor 's3_jar_path' found in config")
        sys.exit(1)
    
    # Get the JAR file path
    jar_file = config.get('s3_jar_path')
    if not jar_file and 'jar_path' in config:
        # Check if JAR exists locally
        jar_path = config['jar_path']
        if not os.path.exists(jar_path):
            print(f"Error: JAR file not found at {jar_path}")
            sys.exit(1)
            
        # Get S3 bucket from config
        if 's3_bucket' not in config:
            print("Error: 's3_bucket' is required in config when using local jar_path")
            sys.exit(1)
            
        # Use the S3 path directly
        s3_bucket = config['s3_bucket']
        jar_file = f"s3://{s3_bucket}/{os.path.basename(jar_path)}"
    
    # Prepare the arguments for the dataExport application
    args = []
    
    # Add the required parameters
    args.extend(["--spark_master", config.get('spark_master', 'spark://spark-master:7077')])  # Added spark_master parameter
    args.extend(["--cassandra_host", config['cassandra_host']])
    args.extend(["--cassandra_port", config['cassandra_port']])
    args.extend(["--output_path", config['output_path']])
    
    # Add optional parameters if provided
    if 'table_list' in config and config['table_list']:
        args.extend(["--table_list", config['table_list']])
    
    if 'jar_packages' in config:
        args.extend(["--jar_packages", config['jar_packages']])
    
    if 'sql_extensions' in config:
        args.extend(["--sql_extensions", config['sql_extensions']])
    
    # Prepare the Spark configuration
    conf = spark_config.get('additional_conf', {})
    
    # Construct the Livy API payload format
    payload = {
        "file": jar_file,
        "className": "com.export.dataExport",
        "args": args,
        "driverMemory": driver_memory.upper(),
        "executorMemory": executor_memory.upper(),
        "driverCores": driver_cores,
        "executorCores": executor_cores,
        "conf": conf
    }
    
    # Print the request payload for debugging
    print("Submitting job with payload:")
    print(json.dumps(payload, indent=2))
    
    # Submit the job to the infra service API
    try:
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        response = requests.post(infra_endpoint, headers=headers, json=payload)
        
        # Check response status
        if response.status_code != 200 and response.status_code != 201:
            print(f"Error submitting job: {response.status_code} - {response.text}")
            sys.exit(1)
        
        # Parse the response to get job ID
        job_info = response.json()
        job_id = job_info.get('id')
        
        print(f"Spark job submitted successfully! Job ID: {job_id}")
        print(f"Job details: {json.dumps(job_info, indent=2)}")
        
        # Return the job ID and the base endpoint (without "/batches") for status monitoring
        base_endpoint = infra_endpoint.replace('/batches', '')
        return job_id, base_endpoint
    except requests.exceptions.RequestException as e:
        print(f"Error submitting Spark job: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        sys.exit(1)

def monitor_job(status_endpoint, job_id):
    """Monitor the status of a submitted Spark job using the DCluster API."""
    if not status_endpoint:
        print("Error: status_endpoint is empty or None")
        return False
        
    if not job_id:
        print("Error: job_id is empty or None")
        return False
            
    print(f"Monitoring job {job_id} using status endpoint: {status_endpoint}")
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(status_endpoint)
            response.raise_for_status()
            
            # According to the API doc, the state endpoint returns a simple state object
            # {"id":4,"state":"success"}
            status_info = response.json()
            
            # Extract state from the response
            state = None
            if isinstance(status_info, dict):
                state = status_info.get('state')
            
            print(f"Job {job_id} status: {state}")
            
            # Check for terminal states
            if state in ['success', 'SUCCESS', 'FINISHED', 'SUCCEEDED', 'complete', 'COMPLETE', 'completed', 'COMPLETED']:
                print(f"Job {job_id} completed successfully!")
                return True
            elif state in ['dead', 'killed', 'failed', 'FAILED', 'KILLED', 'ERROR', 'error']:
                print(f"Job {job_id} ended with state: {state}")
                print(f"Job details: {json.dumps(status_info, indent=2)}")
                return False
            
            # Job is still running, wait and check again
            time.sleep(POLL_INTERVAL)
            
        except requests.exceptions.RequestException as e:
            print(f"Error monitoring job: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            time.sleep(POLL_INTERVAL)
    
    print(f"Timeout monitoring job {job_id} after {MAX_RETRIES} attempts.")
    return False

def destroy_cluster(cluster_name, infra_service_address=None):
    """Destroy a Spark cluster after the job has finished."""
    if not cluster_name:
        print("Error: cluster_name is empty or None")
        return False
    
    # Use provided infra_service_address or default
    service_address = infra_service_address or DEFAULT_INFRA_SERVICE_ADDRESS
        
    destroy_endpoint = f"{service_address}/cluster/destroy/spark/cluster"
    headers = {'Content-Type': 'application/json'}
    payload = {"cluster_name": cluster_name}
    
    print(f"Destroying cluster: {cluster_name}")
    
    try:
        response = requests.post(destroy_endpoint, headers=headers, json=payload)
        response.raise_for_status()
        
        print(f"Cluster {cluster_name} destruction request sent successfully.")
        print(f"Response: {response.text}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error destroying cluster: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return False

def submit_job_local(cluster_info, config):
    """Generate a curl command for submitting a Spark job to be run on a jumpserver."""
    # Extract service address from cluster info
    service_address = cluster_info.get('serviceAddress')
    
    if not service_address:
        print("Error: 'serviceAddress' not found in cluster_info")
        sys.exit(1)
    
    # Prepare the batches endpoint for job submission
    infra_endpoint = f"{service_address}/batches"
    print(f"Service endpoint for job submission: {infra_endpoint}")
    
    # Get spark configuration from config or use defaults
    spark_config = config.get('spark_config', {})
    driver_memory = spark_config.get('driver_memory', '4g')
    executor_memory = spark_config.get('executor_memory', '7g')
    driver_cores = spark_config.get('driver_cores', 2)
    executor_cores = spark_config.get('executor_cores', 1)
    
    # Check if jar_path exists in config
    if 'jar_path' not in config and 's3_jar_path' not in config:
        print("Error: Neither 'jar_path' nor 's3_jar_path' found in config")
        sys.exit(1)
    
    # Get the JAR file path
    jar_file = config.get('s3_jar_path')
    if not jar_file and 'jar_path' in config:
        # Check if JAR exists locally
        jar_path = config['jar_path']
        if not os.path.exists(jar_path):
            print(f"Error: JAR file not found at {jar_path}")
            sys.exit(1)
            
        # Get S3 bucket from config
        if 's3_bucket' not in config:
            print("Error: 's3_bucket' is required in config when using local jar_path")
            sys.exit(1)
            
        # Use the S3 path directly
        s3_bucket = config['s3_bucket']
        jar_file = f"s3://{s3_bucket}/{os.path.basename(jar_path)}"
    
    # Prepare the arguments for the dataExport application
    args = []
    
    # Add the required parameters
    args.extend(["--spark_master", config.get('spark_master', 'spark://spark-master:7077')])  # Added spark_master parameter
    args.extend(["--cassandra_host", config['cassandra_host']])
    args.extend(["--cassandra_port", config['cassandra_port']])
    args.extend(["--output_path", config['output_path']])
    
    # Add optional parameters if provided
    if 'table_list' in config and config['table_list']:
        args.extend(["--table_list", config['table_list']])
    
    if 'jar_packages' in config:
        args.extend(["--jar_packages", config['jar_packages']])
    
    if 'sql_extensions' in config:
        args.extend(["--sql_extensions", config['sql_extensions']])
    
    # Prepare the Spark configuration
    conf = spark_config.get('additional_conf', {})
    
    # Construct the Livy API payload format
    payload = {
        "file": jar_file,
        "className": "com.export.dataExport",
        "args": args,
        "driverMemory": driver_memory.upper(),
        "executorMemory": executor_memory.upper(),
        "driverCores": driver_cores,
        "executorCores": executor_cores,
        "conf": conf
    }
    
    # Create a JSON string for the payload and escape any double quotes for the curl command
    json_payload = json.dumps(payload).replace('"', '\\"')
    
    # Construct the curl command as a single string without line breaks
    curl_cmd = f"curl -X POST {infra_endpoint} --header \"Content-Type:application/json;charset=UTF-8\" --data \"{json_payload}\""
    
    print("\n\nCURL COMMAND FOR JOB SUBMISSION:")
    print("====================================\n")
    print(curl_cmd)
    print("\n====================================\n")
    
    # Generate the monitoring command as well
    print("After submitting the job, use the job ID from the response to monitor the job status:\n")
    monitoring_cmd = (
        "curl -X GET {}/batches/JOB_ID_HERE/state"
    ).format(service_address)
    print(monitoring_cmd)
    
    return None, None  # Return None for job_id and infra_endpoint to skip monitoring


def main():
    args = parse_arguments()
    
    # Load the configuration file
    config = load_config(args.config)
    
    # Get infrastructure service address from config (already validated in load_config)
    infra_service_address = config['infra_service_address']
    
    # Override cluster_id from command line if provided
    cluster_id = args.cluster_id if args.cluster_id else config['cluster_id']
    
    # Wait for the cluster to be ready
    cluster_info = wait_for_cluster_ready(cluster_id, infra_service_address)
    
    # Submit the Spark job
    job_success = False
    job_id = None
    infra_endpoint = None
    try:
        if args.local_mode:
            # In local mode, just print the curl command and exit
            job_id, infra_endpoint = submit_job_local(cluster_info, config)
            print("\nLocal mode: Printed curl command for manual execution on jumpserver.")
            print("No automatic job monitoring will be performed.")
            return
        else:
            # Standard mode: submit the job and monitor it
            job_id, infra_endpoint = submit_spark_job(cluster_info, config)
            if job_id and infra_endpoint:
                # Monitor the job status
                # Use the state endpoint format from the API documentation
                status_endpoint = f"{infra_endpoint}/batches/{job_id}/state"
                job_success = monitor_job(status_endpoint, job_id)
            else:
                job_success = False
    except Exception as e:
        print(f"Error during job submission: {e}")
        job_success = False
    
    # Determine whether to destroy the cluster based on command line args and config
    should_destroy = config.get('destroy_cluster_after_job', True)
    if args.destroy_cluster:
        should_destroy = True
    elif args.no_destroy_cluster:
        should_destroy = False
    
    # Destroy the cluster after the job has finished (successfully or not)
    if should_destroy:
        # Small delay to ensure job is fully processed
        time.sleep(5)
        destroy_cluster(cluster_info['name'], infra_service_address)
    else:
        print("Cluster will not be destroyed as per configuration.")

if __name__ == "__main__":
    main()
