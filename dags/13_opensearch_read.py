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
import pandas as pd


# 2. 환경변수
HOST = Variable.get("HOST")
AUTH = (Variable.get("AUTH_NAME"), Variable.get("AUTH_PW"))
index_name  = 'factory-45-sensor-v1'


# 4-1. 콜백 함수 : 결과 획득
def _searching_proc(**kwargs):
    client = OpenSearch(
        hosts               = [{"host":HOST, "port":443}],
        http_auth           = AUTH,
        http_compress       = True,
        use_ssl             = True,
        verify_certs        = True,
        ssl_assert_hostname = False,
        ssl_show_warn       = False
    )

    # 4-2. opensearch용 쿼리 
    query = {
        "size": 1000,
        "query": {"range":{"timestamp":{"gte": "now-120m"}}}
        # gte : greater then or equal (>=), now-10m : 현재로부터 10분 전  
    }

    # 4-3. 검색 요청
    response = client.search(index=index_name, body=query)
    print('검색 결과', response)
    hits = response['hits']['hits']

    # 4-4. 검색 결과 체크
    if not hits:
        print('조회 결과 없음')
        return
    else:
        print(len(hits))

    # 4-5. 분석 : 요구사항 수행(평균, 최대 등 계산), 이상탐지
    data    = [ hit['_source'] for hit in hits ]
    df      = pd.DataFrame(data)
    print(df.head(1))


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
    #task1
