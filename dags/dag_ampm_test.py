from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dag_ampm_test.py",
    schedule="30 6-9,17-21 * * 1-5",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='gescheit00@naver.com',
        subject='ampm test',
        html_content='ampm test.'
    )