from airflow import DAG
from airflow.operators.python import PythonOperator

import pendulum
import datetime
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *", # daily 6:30
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 파이썬 함수 정의
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit #어떤 함수를 돌릴 것인가?
    )
    
    py_t1
        