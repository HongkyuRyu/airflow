"""
- 예시 1) 매월 말일 수행되는 Dag
    - 변수 START_DATE: 전월 말일
    - 변수 END_DATE: 어제로 env setting
"""


from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # start_date: 전월 말일, end_date: 1일 전
    bash_task_1 = BashOperator(
        task_id="bash_task_1",
        env={
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'

    
    
    
    )
    
    
    


