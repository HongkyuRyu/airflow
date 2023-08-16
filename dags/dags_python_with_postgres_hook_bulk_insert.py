from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="dags_python_with_postgres_hook_bulk_insert",
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        # table이름과 file이름을 받는다.
        postgres_hook.bulk_load(tbl_nm, file_nm)
    
    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable=insert_postgres,
        op_kwargs={
            'postgres_conn_id': 'conn-db-postgres-custom',
            'tbl_nm': 'tbElectriCharging_bulk_1',
            'file_nm': '/opt/airflow/files/tbElectricCharging/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tb_seoul_fast_charger_status.csv'
        }
    )