# @task 데코레이터를 활용해서 dags를 만든다.
# airflow 공식문서 참조

from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1", # 매주 월요일 2시 0분
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)
    
    python_task_1 = print_context("task_decorator 실행!")
        
