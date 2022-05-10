# Run Airflow on Docker
![badge1](https://img.shields.io/badge/language-Python-01B0F0.svg)

This quick-start guide will allow you to quickly start Airflow with CeleryExecutor in Docker. This is the fastest way to start Airflow.

## Running Airflow on Docker
### 1. docker-compose.yaml
To deploy Airflow on Docker Compose, you should fetch docker-compose.yaml.
```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
```

In addition, please include the following element in the postgres service:

```
    ports:
        - "5432:5432"
```

### 2. Initializing environment
On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user. You have to make sure to configure them for the docker-compose:

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 3. Initialize the database
On all operating systems, you need to run database migrations and create the first user account. To do it, run.
```
docker-compose up airflow-init -d
```

### 4. Cleaning-up the environment
The docker-compose we prepare is a “Quick-start” one. It is not intended to be used in production and it has a number of caveats - one of them being that the best way to recover from any problem is to clean it up and restart from the scratch.

The best way to do it is to:

* Run docker-compose down --volumes --remove-orphans command in the directory you downloaded the docker-compose.yaml file
* remove the whole directory where you downloaded the docker-compose.yaml file rm -rf '<DIRECTORY>'
* re-download the docker-compose.yaml file
* re-start following the instructions from the very beginning in this guide

### 5. Running Airflow
Now you can start all services:
```
docker-compose up
```

### 6. Accessing the environment via a browser using the web interface
Check http://localhost:8080


### 7. Airflow connection and Postgres Operator
In Airflow/Admin/Connections
* Conncection ID: postgres_localhost
* Connection type: Postgres
* Host: postgres # or host.docker.internal
* Schema: <database_name>
* Login: airflow
* Password: airflow
* Port: 5432


### 8. Extending the Dockerfile
You can install python requirements to the airflow container:
```
docker build . --tag extending_airflow:latest
```
In the docker-compose.yaml file change the image name:
```
image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}
```
And then, run:
```
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
```

## More info
For more info, please visit:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html


