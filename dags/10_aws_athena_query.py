
# 1. 패키지 호출
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
BUCKET_NAME         = 'de-ai-14-827913617635-ap-northeast-1-an'
ATHENA_DB_NAME      = 'de-ai-14-an1-glue-db'
SRC_TABLE           = 's3_exam_csv_tbl'
TARGET_TABLE        = 'daily_report_tbl'

S3_TARGET_LOC  = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의
with DAG (
    dag_id      = "10_aws_athena_query", 
    description = "athena query 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'athena', 'sql']
) as dag :
    # 4. Task 정의

    # 4-1. s3에서 이전 실행 결과파일 삭제 
    task1   = S3DeleteObjectsOperator(
        task_id         = 'clean_s3_target',
        bucket          = BUCKET_NAME,
        prefix          = f'athena/tbl/{TARGET_TABLE}/',
        aws_conn_id     = 'aws_default'
    )

    # 4-2. Athena에서 이전 실행 테이블 삭제 
    task2   = AthenaOperator(
        task_id         = 'drop_table',
        query           = f'drop table if exists {TARGET_TABLE}',
        database        = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id     = 'aws_default'
    )

    # 4-3. daily_report_tbl 테이블 생성 쿼리
    query = f'''
        CREATE TABLE {TARGET_TABLE}
        WITH (
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        AS
        SELECT
            result,
            COUNT(*) AS result_count,
            AVG(score) AS avg_score,
            MIN(score) AS min_score,
            MAX(score) AS max_score
        FROM {SRC_TABLE}
        GROUP BY result
        ORDER BY result
    '''

    # 4-4. 쿼리 실행 및 결과파일 생성
    task3 = AthenaOperator(
        task_id='create_table_daily_report',
        query=query,
        database=ATHENA_DB_NAME,
        output_location=S3_QUERY_LOG_LOC,
        aws_conn_id='aws_default',
        do_xcom_push=True
    )

    
    # 5. 의존성 정의

    task1 >> task2 >> task3