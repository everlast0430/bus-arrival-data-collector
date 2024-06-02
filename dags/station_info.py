from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import pendulum
import logging
import pandas as pd

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor() 

@task(task_id='py_etl')
def get_station_info():
    import os

    #현재 파일 실제 경로
    logging.info(os.path.realpath(__file__))

    #현재 파일 절대 경로
    logging.info(os.path.abspath(__file__))
    path = os.getcwd() + '/dataset/station_info.csv'
    path = '/opt/airflow/dataset/station_info.csv'

    df = pd.read_csv(f'{path}')
    insert_sql = ''
    schema = 'dev.adhoc'
    table = 'station_info'

    for i in range(len(df)):
        year_month = df.iloc[i]['year_month']
        manage_city = df.iloc[i]['manage_city']
        bus_type = df.iloc[i]['bus_type']
        station_id = df.iloc[i]['station_id']
        station_no = df.iloc[i]['station_no']
        station_name = df.iloc[i]['station_name']
        pass_route_cnt = df.iloc[i]['pass_route_cnt']

        insert_sql += f"INSERT INTO {schema}.temp_{table} VALUES ('{year_month}', '{manage_city}', '{bus_type}', '{station_id}', '{station_no}', '{station_name}', '{pass_route_cnt}');"

    cur = get_Redshift_connection()

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.info(insert_sql)
        raise e

with DAG(
    dag_id="station_info",
    schedule="@once",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['station_info'],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
        
    get_station_info()