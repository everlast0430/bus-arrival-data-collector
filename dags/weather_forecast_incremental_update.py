from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

import pendulum
import datetime


with DAG(
    dag_id="weather_forcast_incremental_update",
    schedule="* 6-8,17-20 * * 1-5",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # 수원 날씨정보 가져오기
    @task(task_id='extract',
          params={'url' : Variable.get("open_weather_api_url"),
                'key' : Variable.get("open_weather_api_key"),
                'city' : "Suwon",
                'lang' : "kr",
                'metric' : "metric"
                })
    def extract(**kwargs):
        params = kwargs.get('params')
        url = params["url"]
        key = params["key"]
        lat = parmas["lat"]
        lon = params["lon"]
        url = url.format(city, key, lang, metric)
        r = requests.get(url)

        return r.json()
    
    # 날씨 정보 전처리
    @task(task_id='transform')
    def transform(**kwargs):
        print(kwargs)

extract >> transform

        