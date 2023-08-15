from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_common',
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False
) as dag:
    """서울시 전기차 충전소 정보(서울시 설치)"""
    tb_seoul_station_status = SeoulApiToCsvOperator(
        task_id = 'tb_seoul_station_status',
        dataset_nm='evChargingStation',
        path='/opt/airflow/files/evChargingStation/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='tb_seoul_station_status.csv'
    )
    """서울시 전기차 급속충전기 정보 현황(서울시 설치)"""
    tb_seoul_fast_charger_status = SeoulApiToCsvOperator(
        task_id = 'tb_seoul_fast_charger_status',
        dataset_nm='tbElectricCharging',
        path='/opt/airflow/files/tbElectricCharging/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='tb_seoul_fast_charger_status.csv'
    )
    """서울시 자동차 전용도로 위치정보(좌표계: GRS80)"""
    tb_road_gis_status = SeoulApiToCsvOperator(
        task_id = 'tb_road_gis_status',
        dataset_nm='SdeTlSprdMtrwy',
        path='/opt/airflow/files/tb_road_gis_status/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='tb_road_gis_status.csv'
    )
    
    tb_seoul_station_status >> tb_seoul_fast_charger_status >> tb_road_gis_status
    