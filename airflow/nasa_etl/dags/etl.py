import json
import os
from typing import Any, Dict
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from pydantic import BaseModel


class PodData(BaseModel):
    title: str = ''
    explanation: str = ''
    url: str = ''
    date: str = ''
    media_type: str = ''
    

load_dotenv()

NASA_API_KEY = os.environ.get("API_KEY", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

with DAG(
    dag_id='nasa_apod_postgres',
    catchup=False,
    default_args=default_args
):
   
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="nasa_postgres_sink")
        
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
            );
        """
        
        postgres_hook.run(create_table_query)
        
        

    extract_pod_data = HttpOperator(
        task_id="extract_apod",
        method="GET",
        http_conn_id="nasa_api",
        endpoint=f"planetary/apod?api_key",
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json(),
        log_response=True,
    )
    
    
    @task
    def transform_apod_data(pod_data: dict) -> Dict[str, Any]:
        data = PodData(**pod_data)
        return data.model_dump()
    
    
    @task
    def load_data_to_postgres(pod_data: Dict[str, Any]):
        postgres_hook=PostgresHook(postgres_conn_id='nasa_postgres_sink')

        ## Define the SQL Insert Query

        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        payload = PodData(**pod_data)    
        postgres_hook.run(insert_query, parameters=(
            payload.title,
            payload.explanation,
            payload.url,
            payload.date,
            payload.media_type
        ))
        
    
    create_table() >> extract_pod_data
    
    api_response = extract_pod_data.output
    
    transformed_data = transform_apod_data(api_response)
    
    load_data_to_postgres(transformed_data)
        
        