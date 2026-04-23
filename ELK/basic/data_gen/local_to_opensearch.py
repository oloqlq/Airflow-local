#######################################
# 로그(데이터) 발생 -> OpenSearch 직접 전송 #
#######################################

# 1. 패키지 호출
from opensearchpy import OpenSearch
from datetime import datetime
import random
import time

# 2. 환경변수
from dotenv import load_dotenv
import os
load_dotenv()

HOST = os.getenv('OPENSEARCH_HOST')
AUTH = (os.getenv('AUTH_NAME'), os.getenv('AUTH_PW'))
print(HOST, AUTH)


'''
# 3. AWS OpenSearch 서비스 클라이언트 연결
client = OpenSearch(
    hosts               = [{"host":HOST, "port":443}],
    http_auth           = AUTH,
    http_compress       = True,
    use_ssl             = True,
    verify_certs        = True,
    ssl_assert_hostname = False,
    ssl_show_warn       = False
)

# 4. 인덱스 생성
index_name      = 'factory-45-sensor-v1' 
if not client.indices.exists(index=index_name):
    client.indices.create(index=index_name)
'''