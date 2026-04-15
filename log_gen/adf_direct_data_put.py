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
