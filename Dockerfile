# airflow 이미지 
FROM apache/airflow:2.8.1
# 패키지 다운을 위해 root
USER root
# apt-get 패키지 리스트를 업데이트하고, 컨테이너 내에서 텍스트 편집을 위해 vim을 설치
RUN apt-get update && apt-get install -y vim
# 보안을 위해 다시 기본 사용자인 airflow로 전환
USER airflow
# 로컬의 requirements.txt 파일을 이미지 안의 / 경로로 복사
COPY requirements.txt /requirements.txt
# 복사한 requirements.txt 파일을 사용하여 필요한 파이썬 라이브러리들을 설치
# --no-cache-dir 옵션은 불필요한 캐시를 남기지 않아 이미지 용량을 줄임
RUN pip install --no-cache-dir -r /requirements.txt