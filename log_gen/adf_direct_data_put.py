'''
- Amazon Data Firehose 에게 직접 데이터를 넣을 샘플
'''


# 1. 패키지
import boto3
import json
import time

# 2. 환경변수
ACCESS_KEY  = ''
SECRET_KEY  = ''
REGION      = 'ap-northeast-1'


# 3. 특정 서비스 클라이언트 생성
def get_client(service_name='firehose', is_in_aws=True):
    if not is_in_aws:
        session = boto3.Session(
            aws_access_key_id = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name         = REGION
            )
        return session.client('firehose')
    return boto3.client('firehose', region_name = REGION)



firehose = get_client()
print( firehose )



# 4. 로그 생성 및 ADF 발송
from run import make_one_log

# 5.로그 1개 생성 -> adf 발송 함수
def send_log():
    # 5-1. 로그 1개 생성
    response = firehose.put_record(
        DeliveryStreamName = 'de-ai-14-an1-kdf-log-to-s3',
        Record = {
            'Data' : make_one_log() + "\n"
        }
    )
    print(f'전송결과 : {response}')
    pass




# 6. 10번 로그 생성 발송
for i in range(10):
    send_log()