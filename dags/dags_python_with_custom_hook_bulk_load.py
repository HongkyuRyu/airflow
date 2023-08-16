from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
    dag_id="dags_python_with_custom_hook_bulk_load",
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(
            table_name=tbl_nm,
            file_name=file_nm,
            delimiter=',',
            is_header=True,
            is_replace=True
        )
    insert_postgres = PythonOperator(
        task_id = "insert_postgres",
        python_callable=insert_postgres,
        op_kwargs={
            'postgres_conn_id' : 'conn-db-postgres-custom',
            'tbl_nm': 'test02',
            'file_nm': '/opt/airflow/files/tbElectricCharging/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tb_seoul_fast_charger_status.csv'
        }
    )