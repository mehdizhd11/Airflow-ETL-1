from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import redis


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1),
}

ETL_DAG = DAG(
    'simple_etl_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)


# Define the extract function
def extract_data(csv_path, redis_host, redis_port, **kwargs):
    df = pd.read_csv(csv_path)

    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # Store the data in Redis
    r.set('extracted_data', df.to_json())


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
        'csv_path': './ETL_dag_1/customers-1000.csv',
        'redis_host': 'localhost',
        'redis_port': 6379
    },
    dag=ETL_DAG,
    retries=3,  # Number of retries for this task
    retry_delay=timedelta(minutes=1),  # Delay between retries for this task
)


# Define the transform function
def transform_data(redis_host, redis_port, **kwargs):
    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # Retrieve the data from Redis
    extracted_data_json = r.get('extracted_data')
    df = pd.read_json(extracted_data_json)

    # Convert all words in all columns to title case
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)

    # Store the transformed data in Redis
    r.set('transformed_data', df.to_json())


transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'redis_host': 'localhost',
        'redis_port': 6379
    },
    dag=ETL_DAG,
)


# Define the load function
def load_data(mongo_uri, database, collection, redis_host, redis_port, **kwargs):
    # Connect to Redis
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # Retrieve the transformed data from Redis
    transformed_data_json = r.get('transformed_data')
    df = pd.read_json(transformed_data_json)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection]

    # Insert the data into MongoDB
    collection.insert_many(df.to_dict('records'))


load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={
        'mongo_uri': 'mongodb://localhost:27017/',
        'database': 'Airflow',
        'collection': 'simple_ETL',
        'redis_host': 'localhost',
        'redis_port': 6379
    },
    dag=ETL_DAG,
)

# Set task dependencies
extract_task >> transform_task >> load_task
