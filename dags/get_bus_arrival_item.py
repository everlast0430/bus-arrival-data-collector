from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import requests
import pendulum
import logging
import xml.etree.ElementTree as ET

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task(task_id='py_extract_and_transform',
      params={'url' : Variable.get("get_bus_arrival_item_url"),
              'key' : Variable.get("get_bus_arrival_item_key"),
              'stationId' : "203000157",
              'routeId' : "200000112"
              })
def extract_and_transform(**kwargs):
    params = kwargs.get('params')
    url = params['url']
    key = requests.utils.unquote(params['key'])
    station_id = params['stationId']
    route_id = params['routeId']

    params = {"serviceKey" : key,
              "stationId" : station_id,
              "routeId" : route_id}
    
    r = requests.get(url, params=params)
    root = ET.fromstring(r.content)

    try:
        msgHeader = root.findall('msgHeader')[0]
        created_at = msgHeader.findall('queryTime')[0].text
        created_at = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S.%f')
        created_at = created_at.strftime('%Y-%m-%d %H:%M:%S')
        result_code = msgHeader.findall('resultCode')[0].text

        if result_code == '0':
        
            for busArrivalItem in root.iter('busArrivalItem'):
                
                flag = busArrivalItem.find('flag').text
                station_order = busArrivalItem.find('staOrder').text

                plate_no1 = busArrivalItem.find('plateNo1').text
                location_no1 = int(busArrivalItem.find('locationNo1').text)
                predict_time1 = int(busArrivalItem.find('predictTime1').text)
                remain_seat_cnt1 = int(busArrivalItem.find('remainSeatCnt1').text)
                
                if busArrivalItem.find('plateNo2').text == None:
                    plate_no2 = None
                    location_no2 = None
                    predict_time2 = None
                    remain_seat_cnt2 = None #default is 0
                    
                else:
                    plate_no2 = busArrivalItem.find('plateNo2').text
                    location_no2 = int(busArrivalItem.find('locationNo2').text)
                    predict_time2 = int(busArrivalItem.find('predictTime2').text)
                    remain_seat_cnt2 = int(busArrivalItem.find('remainSeatCnt2').text)

            data = [{"route_id":route_id, "station_id":station_id, "created_at":created_at,"station_order":station_order,"flag":flag,
                     "plate_no1":plate_no1, "location_no1":location_no1, "predict_time1":predict_time1,"remain_seat_cnt1":remain_seat_cnt1,
                     "plate_no2":plate_no2, "location_no2":location_no2, "predict_time2":predict_time2,"remain_seat_cnt2":remain_seat_cnt2}]
            
        else:
            logging.info(result_code)
            data = [{"route_id":route_id, "station_id":station_id, "created_at":created_at,"station_order":None,"flag":None,
                     "plate_no1":None, "location_no1":None, "predict_time1":None,"remain_seat_cnt1":None,
                     "plate_no2":None, "location_no2":None, "predict_time2":None,"remain_seat_cnt2":None}]

    except Exception as e:
        logging.info(r.text)
        raise e
    
    """
    [{'route_id': '200000112', 'station_id': '203000157', 'created_at': '2024-04-28 13:47:32', 'station_order': '6', 'flag': 'PASS', 
    'plate_no1': '경기70바5771', 'location_no1': 2, 'predict_time1': 2, 'remain_seat_cnt1': 45, 'plate_no2': None, 'location_no2': None, 
    'predict_time2': None, 'remain_seat_cnt2': None}]
    """
    return data 


@task(task_id='py_load')
def load(**kwargs):
    cur = get_Redshift_connection()
    schema = 'dev.adhoc'
    table = 'get_bus_arrival_item'
    
    data = kwargs['ti'].xcom_pull(task_ids='py_extract_and_transform')[0]
    route_id = data['route_id']
    station_id = data['station_id']
    created_date = data['created_at']
    station_order = data['station_order']
    flag = data['flag']
    plate_no1 = data['plate_no1']
    location_no1 = data['location_no1']
    predict_time1 = data['predict_time1']
    remain_seat_cnt1 = data['remain_seat_cnt1']
    plate_no2 = data['plate_no2']
    location_no2 = data['location_no2']
    predict_time2 = data['predict_time2']
    remain_seat_cnt2 = data['remain_seat_cnt2']

    logging.info(data)
    insert_sql = f"INSERT INTO {schema}.{table} VALUES ('{route_id}', '{station_id}', '{created_date}','{station_order}', '{flag}', \
        '{plate_no1}','{location_no1}', '{predict_time1}', '{remain_seat_cnt1}','{plate_no2}', '{location_no2}', '{predict_time2}','{remain_seat_cnt2}')"

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.info(insert_sql)
        raise e

with DAG(
    dag_id="get_bus_arrival_item",
    schedule="*/1 6-9,17-20 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=['bus'],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
        
    extract_and_transform() >> load()
