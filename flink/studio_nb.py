%flink.pyflink

# st_env -> 자체적으로 준비된 객체 사용.
# 1. 입력 테이블 정의
st_env.execute_sql('''
        CREATE TABLE stock_input (
            ticker STRING,
            price DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
        ) WITH (
            "connector" = "kinesis",
            "stream"    = "de-ai-14-an1-kds-stock-input",
            "aws.region"= "ap-northeast-1",
            "scan.stream.initpos" = "LATEST",
            "format"    = "json"
        )
    ''')

# 2. 출력 테이블 정의
st_env.execute_sql('''
        create table stock_input (
            ticker STRING,
            price DOUBLE,
            window_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
        ) WITH (
            "connector" = "kinesis",
            "stream"    = "de-ai-14-an1-kds-stock-input",
            "aws.region"= "ap-northeast-1",
            "scan.stream.initpos" = "LATEST",
            "format"    = "json"
        )
    ''')

# 3. 연산처리(10초단위/ticker로 데이터 그룹화, 평균 집계)
st_env.execute_sql('''
        INSERT INTO stock_output
        SELECT
            ticker,
            AVG(price) as avg_price,
            TUMBLE_END(event_time, INTERVAL '10' SECOND)  as avg_time
        from
            stock_input
        GROUP BY TUMBLE_END(event_time, INTERVAL '10' SECOND), ticker
                      
    ''')