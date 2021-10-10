from datetime import timedelta
from textwrap import dedent
import requests
import pandas as pd 
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def fetch_data(url, output_filepath):

    r = requests.get(url, allow_redirects=True)
    file_name = url.split('/')[-1]
    open(os.path.join(output_filepath, file_name), 'wb').write(r.content)

def process_data(input_filepath, output_filepath):

    mapper_cols = {
        'Country/Other' : 'country',
        'Total Cases' : 'total_cases',
        'Total Deaths' : 'total_deaths',
        'Total Recovered' : 'total_recovered',
        'Active Cases' : 'active_cases',
        'Total Cases/1M pop' : 'total_cases_per_1m_pop',
        'Deaths/1M pop' : 'deaths_per_1m_pop',
        'Total Tests' : 'total_tests',
        'Tests/ 1M pop' : 'tests_per_1m_pop',
        'Population' : 'population'
    }

    for file in os.listdir(input_filepath):
        df = pd.read_csv(os.path.join(input_filepath,file))
        df = df.rename(columns=mapper_cols)
        df['death_rate'] = round(df['total_deaths'] / df['total_cases'],2)
        df.to_csv(os.path.join(output_filepath, file), index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'sample-etl',
    default_args=default_args,
    description='A simple etl pipeline using Airflow',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """
    This is a sample DAG meant for demo purposes. 
    """  
    start = DummyOperator(
        task_id='start'
    )

    fetchData = PythonOperator  (
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={'url': 'https://raw.githubusercontent.com/vincevertulfo/sample-datasets/main/covid.csv', 'output_filepath': '/opt/airflow/data/raw'}
    )

    processData = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        op_kwargs={'input_filepath': '/opt/airflow/data/raw', 'output_filepath': '/opt/airflow/data/cleaned'}
    )

    loadData = PostgresOperator(
        task_id="load_data",
        postgres_conn_id="postgres_default",
        sql="""
            COPY covid
            FROM '/var/lib/postgresql/files/cleaned/{{ params.file_name }}'
            DELIMITER ','
            CSV HEADER;
            """,
        params={'file_name': 'covid.csv'}
    )

    start >> fetchData >> processData >> loadData