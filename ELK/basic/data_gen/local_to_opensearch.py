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
