from operators.seoul_popul_operator import SeoulPoplulationOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'custom_python_0823',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False
) as dag:
    """서울시 실시간 인구 정보"""
    seoul_population = SeoulPoplulationOperator(
        task_id = 'seoul_population',
        dataset_nm='citydata_ppltn',
        path = '/opt/airflow/files/citydata_ppltn/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='seoul_population.csv'
    )
    
    seoul_population