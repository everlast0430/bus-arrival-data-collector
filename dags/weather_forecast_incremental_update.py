from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import requests
import pendulum
import datetime

with DAG(
    dag_id="weather_forcast_incremental_update",
    schedule="10 6-8,17-20 * * 1-5",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['weather']
) as dag:
    
    def get_BigQuery_connection():
        hook = BigQueryHook(gcp_conn_id="bigquery_default")
        return hook.get_conn().cursor()
    
    # 수원 날씨정보 가져오기
    @task(task_id='py_extract',
          params={'url' : Variable.get("open_weather_api_url"),
                'key' : Variable.get("open_weather_api_key"),
                'city' : "Suwon",
                'lang' : "kr",
                'metric' : "metric"
                })
    def extract(**kwargs):
        params = kwargs.get('params')
        city = params['city']
        key = params['key']
        lang = params['lang']
        metric = params['metric']

        url = params['url']
        print(url)
        print(type(url))
        #url = url.format(city, key, lang, metric)
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&lang={lang}&units={metric}"
        r = requests.get(url)

        return r.json()
    
    # 날씨 정보 전처리
    @task(task_id='py_transform')
    def transform(**kwargs):
        value = kwargs['ti'].xcom_pull(task_ids='py_extract')
        city = value['name'] # 'name': 'Suwon-si'
        weather_info = value['weather'] #'weather': [{'id': 803,'main': 'Clouds','description': '튼구름','icon': '04n'}]
        created_at = datetime.fromtimestamp(value['dt']).strftime('%Y-%m-%d %H:%M:%S')
        
        return city, weather_info, created_at

    @task(task_id='py_load')
    def load(**kwargs):

        # cur = get_BigQuery_connection()
        value = kwargs['ti'].xcom_pull(task_ids='py_transform')
        print('value')
        print(value)
        


        # logging.info(create_sql)
        
        # try:
        #     cur.execute(insert_sql)
        #     cur.excute("Commit;")
        # except Exception as e:
        #     cur.excute("Rollback;")
        #     raise # 실패시 명확히 실패했음을 알기위한 용도
    
    extract() >> transform() >> load()

    

        