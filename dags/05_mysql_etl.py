# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# 3-1. 콜백함수 정의

# 2. DAG 정의
with DAG(
    dag_id      = "05_mysql_etl", 
    description = "DAG ETL 구현 실습",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['', ''],
) as dag:
    # 3. task 정의
    t1 = PythonOperator(
        task_id = "extract"
    )
    t2 = PythonOperator(
        task_id = "transform",
    )
    t3 = PythonOperator(
        task_id = "load",
    )

    # 4. 의존성 정의 -> 시나리오별 준비 
    t1 >> t2 >> t3

    pass