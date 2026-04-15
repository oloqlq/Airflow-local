'''
- 로그 생성기 사용 예시
'''
import json
import time
# 현재 워킹디렉토리에서 코드를 작동할때 경로
from log_generator import LogGenerator


log_gen = LogGenerator()

def make_one_log():
   return json.dumps( log_gen.finance(), ensure_ascii=False )






if __name__ == '__main__':
    config = {
       "target_industry":"finance", # finance, iot, ...., game_lol
       "mode":"random", # random or fixed
       "interval":1,    # 초단위
       "total_count":10,# 생성 개수, 
       "loop":False     # 무한대 생성, 작동
    }
    make_log( config )