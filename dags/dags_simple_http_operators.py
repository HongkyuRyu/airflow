from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
# import common.poi_func as poi_array# poi nums
import pendulum


with DAG(
    dag_id="dags_simple_http_operators",
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule="0 18-20 * * *" #매일 오후 6시~8시 1시간 간격으로 실행
) as dag:
    
    """ 서울시 실시간 전기차 충전소/충전기 데이터 정보 """
    # http_conn_id = 'openapi.seoul.go.kr'
    
    
    tb_electric_station_info = SimpleHttpOperator(
        task_id="tb_electric_station_info",
        http_conn_id='openapi.seoul.go.kr',
        endpoint= '{{var.value.apikey_openapi_seoul_go_kr}}/xml/citydata/1/1/광화문·덕수궁',
        method='GET',
        headers={"Content-Type": "application/xml"}
    )
        
    @task(task_id='python_1')
    def python_1(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_electric_station_info')
        import xmltodict
        from pprint import pprint
        response_dict = xmltodict.parse(rslt)
        pprint(response_dict)
    
    tb_electric_station_info >> python_1() 