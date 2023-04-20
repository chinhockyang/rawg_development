# Video Game Analysis (RAWG Database)

## Introduction

This project aims to perform Data Analysis on recent video games (starting from 2018) to identify key patterns and traits among successful recent games, and develop a predictive model that can classify the success or popularity of future games. Using Apache-Airflow, an Extract, Transform, Load and Transform (ETLT) pipeline will be used to fetch  latest data of video games every month from [RAWG](https://rawg.io/apidocs), an online video games database API, into a MySQL Data Warehouse. This Data Warehouse will serve 2 main downstream applications: a Dashboard Application (built using Plotly Dash) to provide data visualisations and analysis, and a Classification Model to classify a game's success.

## Basic Environment Setup

1. Create a virual environment using ```virtualenv venv```
2. Activate virtual environment
3. Install the following libraries:

| Library | Installation | Purpose |
| :------------- |:-------------|:-------------|
| Python=3.8+ | | |
| Pandas | `pip install pandas` | Data Transformation |
| Dot-Env | `pip install python-dotenv` | Store API Token Keys |
| apache-airflow | view detailed instructions below | ETL Pipeline Automation |
| Requests | `pip install requests` | API Requests / Data Extraction |
| PyMySQL | `pip install pymysql` | Connection to MySQL |
| SQLAlchemy | `pip install SQLAlchemy` | SQL Database Operations |
| Scikit-learn | `pip install scikit-learn` | Machine Learning |
| Seaborn | `pip install seaborn` | Data Visualisation
| Plotly | `pip install plotly` | Data Visualisation |
| Dash | `pip install dash` | Dashboard |
| Dash Bootstrap Components | `pip install dash-bootstrap-components` | Dashboard |
| Dash Bootstrap Templates | `pip install dash-bootstrap-templates` | Dashboard |

4. Create a RAWG user account and obtain API key [here](https://rawg.io/login?forward=developer)
5. Create a ```.env``` file in the project directory, and store your RAWG API token as ```RAWG_TOKEN=<YOUR_TOKEN>```

## MySQL Database Setup

1. Create a schema named ```rawg``` in your local MySQL database server.
2. Take note of the port number used for the server, and your username and password.
3. Store the following ```MYSQL_CONNECTION_STRING="mysql+pymysql://<USERNAME>:<PASSWORD>@localhost:<PORT_NO>/rawg"``` in ```.env```.

## Airflow Setup

1. Set up ```AIRFLOW_HOME``` using ```export AIRFLOW_HOME=~/path_leading_to_proj_directory/airflow```. Make sure ```AIRFLOW_HOME``` points to the airflow folder in this project directory

2. Set up ```AIRFLOW_VERSION``` and ```PYTHON_VERSION```

3. Set up ```CONSTRAINT_URL``` using ```CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"```

4. Run ```pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"``` to install apache-airflow

## Airflow Usage

1. Point ```AIRFLOW_HOME``` to this project's airflow directory (using ```export AIRFLOW_HOME=~/path_leading_to_proj_directory/airflow``` or add into local machine's environment variables)

2. Run ```airflow webserver --port 8080 -D``` to start a webserver on http://localhost:8080/

3. Run ```airflow scheduler``` to start the scheduler

## Dashboard Usage

1. Run ```python dashboard.py``` to run Flask app on http://127.0.0.1:8050/
