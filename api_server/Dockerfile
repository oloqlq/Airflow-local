# 도커파일 : 커스텀한 이미지를 생성한다. 
# 이미지
FROM python:3.12.12-slim

# 작업 디렉토리 설정(작업 루트)
WORKDIR /app

# 필요 패키지 설치 - 가상환경은 설치 X (본 컨테이너에서 단독 사용이기 때문; 유일한 서비스)
# uvicorn : fastapi 앞 단에서 서비스 중계 (프록시, 안정적 백엔드 운영 등 web server역할 담당)
# fastapi : 백엔드 구성, WAS (Web Application Server) 역할 담당.
# pydantic : API를 통해서 전달되는 요청 데이터, 응답하는 데이터의 유효성 체크.
RUN pip install fastapi pydantic uvicorn

# 소스코드 복사 (작업 디렉토리에 복사)
COPY main.py .

# 이미지 -> 컨테이너 구성 이후 자동으로 실행시킬 내용 작성
# main : 파일명(모듈명), app : 아직 구성하기 전의 fastapi 객체명
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

