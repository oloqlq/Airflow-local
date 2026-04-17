'''
[상황 가정 : Athena DB, S3 데이터 존재]
- airflow를 통해서 athena 기본 연동

- task1
    - 테이블 생성 -> Location 정보로 특정 CSV가 존재하는 s3 bucket을 지정.
    - 버킷 내 데이터를 쿼리로 액세스
- task2
    - 최신 상태를 유지. 기존 내용은 제거처리
- task3
    - 획득한 데이터를 분석, 결과를 s3에 저장 및 압축.

- 의존성 
    - task1 >> task2 >> task3

- 스케줄
    - daily
'''





with DAG(

    

) as dag: