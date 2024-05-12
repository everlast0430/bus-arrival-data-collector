from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta

import requests
import pendulum
import logging
#import pymysql



# def get_Mysql_connection():
#     conn = pymysql.connect(host='', port=, user=, password=,)
#     return hook.get_conn().cursor()

# 수원 날씨정보 가져오기
@task(task_id='py_extract',
        params={'url' : Variable.get("open_weather_api_url"),
            'key' : Variable.get("open_weather_api_key"),
            'city' : "Suwon",
            'lang' : "kr",
            'metric' : "metric"
            })
def extract(**kwargs):
    print(Variable.get("mysql_connection_info"))
    conn_info = dict(Variable.get("mysql_connection_info"))
    logging.info(conn_info)
    host = conn_info['host']

    params = kwargs.get('params')
    city = params['city']
    key = params['key']
    lang = params['lang']
    metric = params['metric']
    url = params['url']
    #url = url.format(city, key, lang, metric)
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&lang={lang}&units={metric}"
    r = requests.get(url)

    return r.json()

# 날씨 정보 전처리
@task(task_id='py_transform')
def transform(**kwargs):
    value = kwargs['ti'].xcom_pull(task_ids='py_extract')
    city = value['name']
    weather_info = value['weather']
    created_at = datetime.fromtimestamp(value['dt']).strftime('%Y-%m-%d %H:%M:%S')
    
    return city, weather_info, created_at

# 날씨 데이터 적재
@task(task_id='py_load')
def load(**kwargs):
    cur = get_BigQuery_connection()

    value = kwargs['ti'].xcom_pull(task_ids='py_transform')
    city = value[0]
    weather_main = value[1][0]['main']
    created_at = value[2]

    schema = 'OJW_ADHOC'
    table = 'weather_current'
    
    insert_sql = f"INSERT INTO {schema}.{table} VALUE ({city}, {weather_main}, {created_at})"
    logging.info(create_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.excute("ROLLBACK;")
        raise

with DAG(
    dag_id="weather_current_incremental_update",
    schedule="*/10 6-8,17-20 * * 1-5",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['weather'],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    
    extract() >> transform() >> load()

    

        