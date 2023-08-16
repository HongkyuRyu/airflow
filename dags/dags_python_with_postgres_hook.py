from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_postgres_hook",
    start_date=pendulum.datetime(2023, 8, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        
        postgres_hook = PostgresHook(postgres_conn_id)
        # conn 객체 생성 (데이터베이스 연결)
        with closing(postgres_hook.get_conn()) as conn:
            # cursor 객체 생성(쿼리 작성하기 위함)
            with closing(conn.cursor()) as cursor:
                # task instance 객체에 속하는 value들을 가져온다.
                dag_id = kwargs.get("ti").dag_id
                task_id = kwargs.get("ti").task_id
                run_id = kwargs.get("ti").run_id
                msg = "hook insert 수행"
                sql = 'insert into py_opr_drct_insert values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
    # 파이썬 오퍼레이터 작성
    insert_postgres_with_hook = PythonOperator(
        task_id = 'insert_postgres_with_hook',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom'}
    ) 
    
    insert_postgres_with_hook