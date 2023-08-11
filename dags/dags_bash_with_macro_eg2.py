"""
- 예시 2) **매월 둘째 주 토요일**에 수행되는 Dag
    - 변수 START_DATE: 2주 전 월요일
    - 변수 END_DATE: 2주 전 토요일로 env setting
"""
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2", # 1: 월요일
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 2주 전 월요일, END_DATE: 2주 전 토요일
    bash_task_2 = BashOperator(
        task_id="bash_task_2",
        # 배치일 기준 -14-5 = -19, -14
        env={
            'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}'
        },
        bash_command='echo "START_DATE: $START_DATE" && "END_DATE: $END_DATE"'
    ) 