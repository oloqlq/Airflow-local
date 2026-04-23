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
load_dotenv(override=True)
HOST = os.getenv('OPENSEARCH_HOST')
AUTH = (os.getenv('AUTH_NAME'), os.getenv('AUTH_PW'))

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

# 5. 데이터 생성, 전송
#    스마트팩토리 : 공장에 장비는 3대, 데이터 전송 간격은 1초.
oven_ids = ['OVEN-001', 'OVEN-002', 'OVEN-003']
while True:
    for oven_id in oven_ids:
        temp = random.uniform(200,220)      # 정상범위
        if random.random() > 0.95:          # 0.0 ~ 1.0 사이 값 중 5% 확률로 이상치 생성
            temp += random.uniform(30, 50)  # 임의로 온도 증가 
        
        # json(반정형) 형태의 데이터 구성
        doc = {
            'timestamp'     :datetime.now,
            'oven_id'       :oven_id,
            'temperature'   :round(temp, 2),
            'vibration'     :round(random.uniform(0, 0.15), 2),
            'status'        :'DANGER' if temp >= 240 else 'NORMAL'
        }

        # 전송 : HTTP post
        response = client.index(
            index   = ndex_name,
            body    = doc,
            refresh = True
        )
        print(f"{oven_id} 온도: {doc['temperature']}    전송 완료")
        #pass

    time.sleep(1)
