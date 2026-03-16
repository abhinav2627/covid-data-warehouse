from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
import requests
import io
import subprocess

default_args = {
    'owner': 'abhinav',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def load_raw_covid():
    print('Downloading COVID data...')
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text))
    cols = [
        'iso_code', 'continent', 'location', 'date',
        'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
        'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
        'population', 'median_age', 'gdp_per_capita'
    ]
    df = df[cols]
    df = df[df['continent'].notna()]
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df = df.fillna(0)
    print(f'Rows to load: {len(df):,}')
    conn = snowflake.connector.connect(
        account='YOUR_ACCOUNT',
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        warehouse='COVID_WH',
        database='COVID_DW',
        schema='RAW',
        role='ACCOUNTADMIN'
    )
    cur = conn.cursor()
    cur.execute('''
        CREATE OR REPLACE TABLE COVID_DW.RAW.RAW_COVID_DATA (
            ISO_CODE VARCHAR, CONTINENT VARCHAR, LOCATION VARCHAR, DATE DATE,
            TOTAL_CASES FLOAT, NEW_CASES FLOAT, TOTAL_DEATHS FLOAT, NEW_DEATHS FLOAT,
            TOTAL_VACCINATIONS FLOAT, PEOPLE_VACCINATED FLOAT,
            PEOPLE_FULLY_VACCINATED FLOAT, POPULATION FLOAT,
            MEDIAN_AGE FLOAT, GDP_PER_CAPITA FLOAT
        )
    ''')
    chunk_size = 5000
    total = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        rows = [tuple(row) for row in chunk.itertuples(index=False)]
        cur.executemany(
            'INSERT INTO COVID_DW.RAW.RAW_COVID_DATA VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            rows
        )
        total += len(rows)
        print(f'  Loaded {total:,} rows...')
    conn.commit()
    cur.close()
    conn.close()
    print(f'Done! {total:,} rows loaded')

def run_dbt_staging():
    result = subprocess.run(
        ['/usr/local/bin/dbt', 'run', '--select', 'staging',
         '--project-dir', '/usr/local/airflow/dbt/covid_dw',
         '--profiles-dir', '/usr/local/airflow'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt staging failed: {result.stderr}')

def run_dbt_marts():
    result = subprocess.run(
        ['/usr/local/bin/dbt', 'run', '--select', 'marts',
         '--project-dir', '/usr/local/airflow/dbt/covid_dw',
         '--profiles-dir', '/usr/local/airflow'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt marts failed: {result.stderr}')

def run_dbt_test():
    result = subprocess.run(
        ['/usr/local/bin/dbt', 'test',
         '--project-dir', '/usr/local/airflow/dbt/covid_dw',
         '--profiles-dir', '/usr/local/airflow'],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f'dbt test failed: {result.stderr}')

with DAG(
    dag_id='covid_data_pipeline',
    default_args=default_args,
    description='End-to-end COVID pipeline: RAW load + dbt transforms',
    schedule='@daily',
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['covid', 'snowflake', 'dbt', 'portfolio'],
) as dag:

    task_load_raw = PythonOperator(
        task_id='load_raw_covid_data',
        python_callable=load_raw_covid,
    )
    task_dbt_staging = PythonOperator(
        task_id='dbt_run_staging',
        python_callable=run_dbt_staging,
    )
    task_dbt_marts = PythonOperator(
        task_id='dbt_run_marts',
        python_callable=run_dbt_marts,
    )
    task_dbt_test = PythonOperator(
        task_id='dbt_test',
        python_callable=run_dbt_test,
    )

    task_load_raw >> task_dbt_staging >> task_dbt_marts >> task_dbt_test
