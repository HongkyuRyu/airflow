from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_template",
    schedule="30 9 * * *", # 매일 9시 30분
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    # 시작날짜 8/1일이고, 오늘 날짜가 8/11일이니까
    catchup=True
    # 8/1 ~ 8/11일까지 모두 수행
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    show_templates()
