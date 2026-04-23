#######################################
#  DAG에서 opensearch 검색 -> 데이터 획득  #
#######################################


# 1. 패키지 호출
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from opensearchpy import OpenSearch
import pendulum # 시간대 간편 설정
from airflow.models import Variable



# 2. 환경변수
HOST = Variable.get("HOST")
AUTH = (Variable.get("AUTH_NAME"), Variable.get("AUTH_PW"))
print( HOST, AUTH)


# 4-1. 콜백 함수 : 결과 획득
def _searching_proc(**kwargs):
    pass


# 3. DAG 정의
with DAG(
    dag_id      = "13_opensearch_read", 
    description = "DAG에서 opensearch활용",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '*/10 * * * *', # 00시00분00초에 스케줄 작동
    start_date  = pendulum.datetime(2026,1,1,tz="Asia/Tokyo"),     
    catchup     = False,
    tags        = ['aws', 'opensearch']
    
) as dag:
    # 4. Task 정의
    task1 = PythonOperator(
        task_id='searching_proc',
        python_callable = _searching_proc
    )

    # 5. 의존성
    task1
