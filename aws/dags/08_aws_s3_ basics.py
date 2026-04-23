'''
- 원격 PC에서 AWS S3에 데이터를 업로드하는 간단한 DAG
    - 엑세스키가 잘 작동하는지 체크
    - 데이터량에 다른 수행시간 체크 -> 데이터를  S3에 적제하는 방식에 대한 고민 (직접 or 서비스 이용)    
- 설치 (호스트 PC, 로컬 PC상)
    - pip install apache-airflow-providers-amazon
'''
# 1. 모듈가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# 2. 환경변수 설정
BUCKET_NAME = "de-ai-14-827913617635-ap-northeast-1-an" 
FILE_NAME   = 'hello.txt' 
LOCAL_PATH  = f'/opt/airflow/dags/data/{FILE_NAME}'

def _check_s3(**kwargs):
    hook = S3Hook(aws_conn_id='aws_default')
    keys = hook.list_keys(bucket_name=BUCKET_NAME)
    if not keys:
        raise ValueError('업로드 실패') 
    for key in keys:
        logging.info(f'키 : {key}')

    pass

# 3. DAG 정의
with DAG(
    dag_id      = "08_aws_s3_basics", 
    description = "aws 연동, s3 업로드",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3'],
)as dag:
    # 4. Task 정의 
    task_create_file = BashOperator(
        task_id = "create_file",
        bash_command = f'echo "hello airflow & s3" > {LOCAL_PATH}'
    )
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id  = "upload_to_s3",
        filename = LOCAL_PATH,  
        dest_key = FILE_NAME,   
        dest_bucket = BUCKET_NAME,  
        aws_conn_id = 'aws_default', 
        replace  = True      
    )
    task_check_s3     = PythonOperator(
        task_id = "check_s3",
        python_callable = _check_s3
    )

    # 5. 의존성
    task_create_file >> task_upload_to_s3 >> task_check_s3
    pass
