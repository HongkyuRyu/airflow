from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
# from plugins.common.common_func import get_sftp
# 이렇게 하면 airflow에서는 에러가 발생한다. 따라서 .env로 최상위폴더를 다시 설정해주었다.
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp
    )