from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

#lati and longi for London
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_fault'
API_CONN_ID = 'open_meteo_api'

default_args ={
    'owner': 'airflow',
    'start_date': days_ago(1)
}

#dag implementation
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval = '@daily',
         catchup = False) as dags:
    
    @task()
    def extract_weather_data():
        """ Extract weather from open-meteo api using airflow connection"""

        #use HTTPhook to get details for airflow connection
        https_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')

        #Build an endpoint
        #https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        #making the request via http hook
        response = https_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            return Exception(f"Failed to fetch the weather data : {response.status_code}")
    
    @task()
    def tranform_weather_data(weather_data):
        """Tranform extracted weather data"""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : LATITUDE,
            'longitude' : LONGITUDE,
            'temperature' : current_weather['temperature'], 
            'windspeed': current_weather['windspeed'], 
            'winddirection': current_weather['winddirection'], 
            'weathercode': current_weather['weathercode'] 
        }
        return transformed_data
    
    @task
    def load_weather_data(transformed_data):
        """Load tranformed data into postgres"""
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        """Create table if it does not exist"""
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data(
                       latitude FLOAT,
                       longitude FLOAT,
                       temperature FLOAT,
                       windspeed FLOAT,
                       winddirection FLOAT,
                       weathercode INT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       );
        """)

        """Insert data"""
        cursor.execute("""
        INSERT INTO weather_data (latitude,longitude,temperature,windspeed,winddirection,weathercode)
        VALUES(%s,%s,%s,%s,%s,%s)
        """,(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
        ))

        conn.commit()
        cursor.close()

    #DAG WORKFLOW - ETL Pipeline
    weather_data=extract_weather_data()
    transformed_data=tranform_weather_data(weather_data)
    load_weather_data(transformed_data)

### for deployment, go to AWS, create a databse and then use the database 
### endpoint and add in airflow connections(edit postgres and change the hosts)