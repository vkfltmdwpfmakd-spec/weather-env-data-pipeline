from __future__ import annotations
import os
import pendulum # 날짜/시간 더 쉽게 다루기 위한
import pandas as pd # 데이터 분석 및 조작
import requests # HTTP 요청 - API요청 
from airflow.decorators import dag, task # Airflow DAG와 Task를 쉽게 만들기 위한
from airflow.models.variable import Variable # Airflow UI의 Variables를 가져오기 위한
from minio import Minio # MinIO(S3)와 통신하기 위한
from datetime import datetime # 파이썬 기본 날짜/시간
import logging # 로그를 기록하기 위한

# DAG 정의: 파이프라인의 전체 구조와 속성을 설정
@dag(
    dag_id="KMA_SHORT_FORECAST_PIPELINE",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False, # 놓친 스케줄을 실행하지 않도록 설정
    tags=["weather, kma, forecast"] # Airflow UI에서 DAG를 쉽게 찾기 위한 태그
)




def KMA_SHORT_FORECAST_PIPELINE():
    """
        기상 데이터 수집 및 MinIO 저장 파이프라인
        1. Airflow UI에 저장된 기상청 API 키를 Variable로 읽어옴
        2. 기상청 단기예보 API에서 데이터를 JSON 형태로 가져옴
        3. JSON 데이터를 Pandas DataFrame으로 변환 후 CSV 파일로 저장
        4. 생성된 CSV 파일을 MinIO 'kma_short_forecast' 버킷에 업로드
    """
        
    # Task 정의: 파이프라인 내에서 실제 작업을 수행하는 함수
    @task
    def extract_and_save_to_minio():
        service_key = Variable.get("KMA_SHORT_FORECAST_KEY") # API KEY 설정

        # base_date = "20250824"
        base_date = datetime.now().strftime("%Y%m%d") # 현재 날짜
        base_time = "0600" # 발표시각 6시
        # 하드코딩이 아닌 여러 지역의 데이터를 가져 올 수 있도록 할 예정 [디버깅]
        nx = "55" # 예보지점 X 좌표
        ny = "127" # 예보지점 Y 좌표

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst' # API URL

        # API 요청 파라미터 설정
        params = {
            'serviceKey': service_key,
            'pageNo': '1',
            'numOfRows': '1000',
            'dataType': 'JSON', # 설정 안할 시 XML
            'base_date': base_date,
            'base_time': base_time,
            'nx': nx,
            'ny': ny
        }

        try:
            response = requests.get(url, params=params, timeout=30) # API 요청
            response.raise_for_status() # 200번대 응답 코드가 아니면 에러를 발생

            # JSON 응답에서 실제 데이터가 있는 'item' 리스트를 추출
            items = response.json().get("response", {}).get("body", {}).get("items", {}).get("item", [])

            if not items:
                logging.info(f"'{base_date}' 단기예보 데이터가 없습니다.")
                return

            df = pd.DataFrame(items) # JSON 데이터를 DataFrame으로 변환

            file_path =f"/opt/airflow/data/kma_short_forecast_{base_date}.csv" # CSV 파일 경로 설정
            df.to_csv(file_path, index=False) # DataFrame을 CSV 파일로 저장

            logging.info(f"CSV 파일 저장완료 : {file_path}")

            # MinIO 클라이언트 생성 , airflow Connection 에서 불러오도록 할 예정 [디버깅]
            client = Minio(
                "minio:9000", # Docker Compose 환경 , 로컬 직접 실행시 "localhost:9000" 
                access_key="minio",
                secret_key="minio1234",
                secure=False # HTTP 사용 (개발환경)
            ) 

            bucket_name = "kma-short-forecast" # 데이터를 저장할 버킷 이름
            object_name = f"raw/kma_short_forecast_{base_date}.csv" # 버킷 내에 저장될 파일 경로와 이름

            if not client.bucket_exists(bucket_name): # 버킷이 존재하지 않으면
                client.make_bucket(bucket_name) # 버킷 생성
                logging.info(f"버킷 생성완료 : {bucket_name}")

            client.fput_object(bucket_name, object_name, file_path) # 버킷에 업로드
            logging.info(f"파일 업로드완료 : {bucket_name}/{object_name}")

        # 에러 관련 사항들을 세세하게 나눠서 관리 할 수 있도록 할 예정 [디버깅]
        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 오류: {e}")
            raise # 에러를 다시 발생시켜 Airflow가 태스크를 실패 처리하도록 함
        except Exception as e:
            logging.error(f"데이터 처리 또는 MiniO 업로드 실패: {e}")
            raise
    
    extract_and_save_to_minio()

KMA_SHORT_FORECAST_PIPELINE() # DAG 실행