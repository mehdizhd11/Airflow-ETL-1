# README

## Overview

This project implements a simple ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline consists of three main tasks:

1. **Extract Data**: Reads data from a CSV file and stores it in Redis.
2. **Transform Data**: Retrieves the extracted data from Redis, transforms it by converting all text to title case, and stores the transformed data back in Redis.
3. **Load Data**: Retrieves the transformed data from Redis and loads it into a MongoDB collection.

## Prerequisites

To run this ETL pipeline, you need to have the following installed:

- Python (>=3.6)
- Apache Airflow
- Redis
- MongoDB
- Required Python packages (install via `pip`):
  - pandas
  - pymongo
  - redis
  - apache-airflow

## Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install required Python packages**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Redis**:
   - Install Redis from https://redis.io/download
   - Start the Redis server:
     ```bash
     redis-server
     ```

4. **Set up MongoDB**:
   - Install MongoDB from https://www.mongodb.com/try/download/community
   - Start the MongoDB server:
     ```bash
     mongod
     ```

5. **Prepare the CSV file**:
   - Place your CSV file (`customers-1000.csv`) in the project directory.

## Configuration

The DAG (Directed Acyclic Graph) is defined in the `simple_etl_dag.py` file. The default arguments and DAG configuration are as follows:

- **Owner**: airflow
- **Email on Failure/Retry**: Enabled
- **Retries**: 1 (global), 3 (extract task specific)
- **Retry Delay**: 1 minute
- **Start Date**: January 1, 2023
- **Schedule Interval**: Daily

## ETL Tasks

### Extract Data

This task reads data from a CSV file and stores it in Redis.

```python
def extract_data(csv_path, redis_host, redis_port, **kwargs):
    df = pd.read_csv(csv_path)
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    r.set('extracted_data', df.to_json())
```

### Transform Data

This task retrieves the extracted data from Redis, converts all text to title case, and stores the transformed data back in Redis.

```python
def transform_data(redis_host, redis_port, **kwargs):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    extracted_data_json = r.get('extracted_data')
    df = pd.read_json(extracted_data_json)
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)
    r.set('transformed_data', df.to_json())
```

### Load Data

This task retrieves the transformed data from Redis and loads it into a MongoDB collection.

```python
def load_data(mongo_uri, database, collection, redis_host, redis_port, **kwargs):
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    transformed_data_json = r.get('transformed_data')
    df = pd.read_json(transformed_data_json)
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection]
    collection.insert_many(df.to_dict('records'))
```

## Running the Pipeline

1. **Start the Airflow Scheduler and Webserver**:
   ```bash
   airflow scheduler
   airflow webserver
   ```

2. **Trigger the DAG**:
   - Open the Airflow web UI (default: http://localhost:8080)
   - Trigger the `simple_etl_dag` DAG

## License

This project is licensed under the MIT License.

## Acknowledgments

- [Apache Airflow](https://airflow.apache.org/)
- [Redis](https://redis.io/)
- [MongoDB](https://www.mongodb.com/)

For any questions or issues, please open an issue in the repository.
