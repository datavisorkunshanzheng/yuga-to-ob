# DCluster API Documentation for Spark

This document outlines the available API endpoints for managing Spark clusters and jobs.

## Table of Contents
- [Cluster Management](#cluster-management)
  - [Launch Cluster](#launch-cluster)
  - [Get Cluster Status](#get-cluster-status)
  - [Terminate Cluster](#terminate-cluster)
- [Job Management](#job-management)
  - [Submit Job](#submit-job)
  - [Get Job Status](#get-job-status)
  - [Kill Job](#kill-job)

## Cluster Management

### Launch Cluster

Creates a new Spark cluster with the specified configuration.

**Endpoint:**
```
POST https://dcluster-awsuswest2deva-qa-oneclick.dv-api.com/cluster/v2/launch/spark/cluster
```

**Headers:**
```
Content-Type: application/json
```

**Request Body:**
```json
{
    "workers": 4,
    "workerMemory": "6",
    "workerCpu": "1",
    "masterMemory": "4",
    "masterCpu": "1",
    "tenant": "test"
}
```

**Example:**
```bash
curl --location --request POST 'https://dcluster-awsuswest2deva-qa-oneclick.dv-api.com/cluster/v2/launch/spark/cluster' \
--header 'Content-Type: application/json' \
--data '{
    "workers": 4,
    "workerMemory": "6",
    "workerCpu": "1",
    "masterMemory": "4",
    "masterCpu": "1",
    "tenant": "test"
}'
```

**Response:**
```
clusterId
```

### Get Cluster Status

Returns the current status of a specified Spark cluster.

**Endpoint:**
```
GET https://dcluster-awsuswest2deva-infra.dv-api.com/cluster/status/spark/cluster/{clusterId}
```

**Example:**
```bash
curl --location --request GET 'https://dcluster-awsuswest2deva-infra.dv-api.com/cluster/status/spark/cluster/2'
```

**Response:**
```json
{
    "clusterIps": [
        "http://spark-master.spark-inf-test-2.svc.cluster.local:8998"
    ],
    "serviceAddress": "http://spark-master.spark-inf-test-2.svc.cluster.local:8998",
    "interrupted": false,
    "workerMemory": "6",
    "masterMemory": "4",
    "name": "spark-inf-test-2",
    "namespace": "spark-inf-test-2",
    "workerCpu": "1",
    "id": 2,
    "workers": 4,
    "tenant": "test",
    "masterCpu": "1",
    "status": "STARTING"
}
```

### Terminate Cluster

Terminates a specified Spark cluster.

**Endpoint:**
```
DELETE https://dcluster-awsuswest2deva-infra.dv-api.com/cluster/terminate/spark/{clusterId}
```

**Example:**
```bash
curl --location --request DELETE 'https://dcluster-awsuswest2deva-infra.dv-api.com/cluster/terminate/spark/0'
```

## Job Management

### Submit Job

Submits a job to a running Spark cluster.

**Endpoint:**
```
POST {serviceAddress}/batches
```

**Headers:**
```
Content-Type: application/json;charset=UTF-8
```

**Request Body:**
```json
{
    "args": [
        "--spark_master",
        "spark://spark-master:7077",
        "--cassandra_host",
        "aws-useast1-obtest-yb-d36647071f4975e5.elb.us-east-1.amazonaws.com",
        "--cassandra_port",
        "9042",
        "--jar_packages",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
        "--sql_extensions",
        "com.datastax.spark.connector.CassandraSparkExtensions",
        "--output_path",
        "s3a://datavisor-preprod-qatestpre-awsuseast1prod/data",
        "--table_list",
        "dv.velocity_galileo"
    ],
    "file": "s3a://datavisor-preprod-qatestpre-awsuseast1prod/export_db-1.0-SNAPSHOT.jar",
    "className": "com.export.dataExport",
    "driverMemory": "4G",
    "driverCores": 2,
    "executorMemory": "7G",
    "executorCores": 1,
    "conf": {
        "spark.master": "spark://spark-master:7077",
        "spark.submit.deployMode": "client",
        "spark.default.parallelism": "20",
        "spark.sql.shuffle.partitions": "20",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
}
```

**Example:**
```bash
curl -X POST http://spark-master:8998/batches \
--header "Content-Type:application/json;charset=UTF-8" \
--data '{
  "file": "s3a://datavisor-dev/spark-examples_2.12-3.3.2.jar",
  "className": "org.apache.spark.examples.SparkPi",
  "executorMemory": "4G",
  "executorCores": 1,
  "args": ["100"]
}'
```

**Response:**
```json
{
  "id": 4,
  "name": null,
  "owner": null,
  "proxyUser": null,
  "state": "running",
  "appId": null,
  "appInfo": {
    "driverLogUrl": null,
    "sparkUiUrl": null
  },
  "log": [
    "stdout: ",
    "\nstderr: "
  ]
}
```

### Get Job Status

Returns the current status of a specified job.

**Endpoint:**
```
GET {serviceAddress}/batches/{id}/state
```

**Example:**
```bash
curl http://spark-master:8998/batches/4/state
```

**Response:**
```json
{
  "id": 4,
  "state": "running"
}
```

**Possible states:**
- `starting`
- `running`
- `success`
- `dead`

### Kill Job

Terminates a running job.

**Endpoint:**
```
DELETE {serviceAddress}/batches/{id}
```

**Example:**
```bash
curl -X DELETE http://spark-master:8998/batches/5
```

**Response:**
```json
{
  "msg": "deleted"
}
```

