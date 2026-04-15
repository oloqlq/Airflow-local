'''
- 평시 -> 잠복하듯이 센서를 켜고 대기중
- 특정 버킷 혹은 버킷내 공간을 감시(sensor) -> 파일(객체등) 업로드 -> 감지 -> DAG 작동
'''

# 1. 패키지
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # s3 키등 읽는 용도
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 감시용 센서
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정데이터(객체) 삭제
import logging


# 2. 환경변수
# 버킷명, 파일명, s3 키 -> 버킷 안의 키가 변화했는지만 확인
BUCKET_NAME     = "de-ai-14-827913617635-ap-northeast-1-an" 
FILE_NAME       = 'sensor_data.csv'
S3_KEY          = f'name/{FILE_NAME}'


# 4-1. 콜백 함수
def
def 



# 3. DAG 정의
with DAG(
    dag_id      = "09_aws_s3_consummer", 
    description = "s3의 특정 버킷에 대해, 데이터 변화 1.감지 -> 2.읽기 -> 3.처리 -> 4.삭제",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None,
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3'],
) as dag:
    # 4. Task 정의
    # 감지
    task_waiting_trigger = S3KeySensor()
    # 읽기(처리 생략)
    task_reading_data = PythonOperator()
    # 삭제 (or 백업)
    task_delete_data_or_backup = S3DeleteObjectsOperator()


    # 5. 의존성
    # 센서 감지 -> 작업 -> 키 제거 -> 초기화
