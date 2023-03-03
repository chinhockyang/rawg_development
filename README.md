# Development Repository for RAWG Data Pipeline

## Basic Setup

1. Create a virual environment using ```virtualenv venv```
2. Activate virtual environment (tutorial slides)
3. Install the following libraries:
- pandas
- requests
- python-dotenv
- sqlalchemy
- pymysql
- apache-airflow (view detailed instructions below)
4. Create a .env file in the project directory, and store your RAWG API token as ```RAWG_TOKEN=<YOUR_TOKEN>```
5. Should be able to use the notebooks. (Don't run all cells at once if not might waste API requests)

## MySQL Database Setup

1. Create a schema named ```rawg``` in your local MySQL database server.
2. Take not of the port number used for the server, and your username and password.
3. Store the following ```MYSQL_CONNECTION_STRING="mysql+pymysql://<USERNAME>:<PASSWORD>@localhost:<PORT_NO>/rawg"``` in ```.env``` folder.

## Airflow Setup

1. Set up ```AIRFLOW_HOME``` using ```export AIRFLOW_HOME=~/path_leading_to_proj_directory/airflow```. Make sure ```AIRFLOW_HOME``` points to the airflow folder in this project directory

2. Set up ```AIRFLOW_VERSION``` and ```PYTHON_VERSION```

3. Set up ```CONSTRAINT_URL``` using ```CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"```

4. Run ```pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"``` to install apache-airflow

## Airflow Usage

1. Point ```AIRFLOW_HOME``` to this project's airflow directory (using ```export AIRFLOW_HOME=~/path_leading_to_proj_directory/airflow``` or add into local machine's environment variables)

2. Run ```airflow webserver --port 8080 -D``` to start a webserver on http://localhost:8080/

3. Run ```airflow scheduler``` to start the scheduler

4. Ensure that the ```dags_folder``` variable in airflow/airflow.cfg file points to the ```dags``` folder within the airflow folder in this project directory