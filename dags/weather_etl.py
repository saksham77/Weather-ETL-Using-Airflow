from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Retrieve the API key from environment variables or set it directly
API_KEY = os.getenv('OPENWEATHER_API_KEY')  # Replace 'your_api_key' with actual key if not using environment variable
print(API_KEY)

def fetch_weather_data():
    # Fetch weather data for London, UK
    response = requests.get(
        f'http://api.openweathermap.org/data/2.5/weather?q=London,uk&appid={API_KEY}&units=metric'
    )
    data = response.json()
    return data

def transform_weather_data(**kwargs):
    # Retrieve the fetched data from previous task
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_data')
    
    # Transform the data
    transformed_data = {
        'city': fetched_data['name'],
        'temperature': fetched_data['main']['temp'],
        'humidity': fetched_data['main']['humidity'],
        'wind_speed': fetched_data['wind']['speed'],
        'weather_description': fetched_data['weather'][0]['description']
    }
    
    # Push the transformed data to XCom
    ti.xcom_push(key='transformed_data', value=transformed_data)

def generate_insert_sql(**kwargs):
    # Retrieve transformed data from XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    # Construct the SQL command
    insert_sql = f"""
    INSERT INTO public.weather (city, temperature, humidity, wind_speed, weather_description)
    VALUES ('{transformed_data['city']}', {transformed_data['temperature']}, {transformed_data['humidity']}, {transformed_data['wind_speed']}, '{transformed_data['weather_description']}')
    """
    
    # Push the SQL command to XCom for the next task to retrieve
    ti.xcom_push(key='insert_sql', value=insert_sql)

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to load weather data into Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to fetch data from OpenWeatherMap API
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_weather_data
    )

    # Task to transform data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_weather_data,
        provide_context=True
    )

    # Task to create Snowflake database and schema
    create_database = SnowflakeOperator(
        task_id='create_database',
        sql='CREATE DATABASE IF NOT EXISTS airflow_db',
        snowflake_conn_id='snowflake_conn',  # Define Snowflake connection in Airflow UI
    )

    create_schema = SnowflakeOperator(
        task_id='create_schema',
        sql='CREATE SCHEMA IF NOT EXISTS public',
        snowflake_conn_id='snowflake_conn',
        database='airflow_db',  # Use the newly created database
    )

    # Task to create table in Snowflake
    create_table = SnowflakeOperator(
        task_id='create_table',
        sql='''
        CREATE TABLE IF NOT EXISTS public.weather (
            city VARCHAR,
            temperature FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            weather_description VARCHAR
        )
        ''',
        snowflake_conn_id='snowflake_conn',
        database='airflow_db',
        schema='public',
    )

    # Task to generate the SQL for inserting data
    generate_sql = PythonOperator(
        task_id='generate_sql',
        python_callable=generate_insert_sql,
        provide_context=True,
    )

    # Task to load data into Snowflake
    load_data = SnowflakeOperator(
        task_id='load_data',
        sql="{{ task_instance.xcom_pull(task_ids='generate_sql', key='insert_sql') }}",
        snowflake_conn_id='snowflake_conn',
        database='airflow_db',
    )

    # Define task dependencies
    fetch_data >> transform_data >> create_database >> create_schema >> create_table >> generate_sql >> load_data
