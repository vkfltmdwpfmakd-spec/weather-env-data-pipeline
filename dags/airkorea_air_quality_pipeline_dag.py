from __future__ import annotations
import os
import pendulum # 날짜/시간 더 쉽게 다루기 위한
import pandas as pd # 데이터 분석 및 조작
import requests # HTTP 요청 - API요청 
from airflow.decorators import dag, task # Airflow DAG와 Task를 쉽게 만들기 위한
from airflow.models.variable import Variable # Airflow UI의 Variables를 가져오기 위한
from minio import Minio # MinIO(S3)와 통신하기 위한
from datetime import datetime, timedelta # 파이썬 기본 날짜/시간
import logging # 로그를 기록하기 위한

@dag(
    dag_id = "AIRKOREA_AIR_QUALITY_PIPELINE",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags = ["weather", "airkorea"]
)

def AIRKOREA_AIR_QUALITY_PIPELINE():
    """
        대기질 데이터 수집 및 MinIO 저장 파이프라인
        1. Airflow UI에 저장된 에어코리아 API 키를 Variable로 읽어옵니다.
        2. 측정소별 실시간 측정정보 API에서 데이터를 JSON 형태로 가져옵니다.
        3. JSON 데이터를 Pandas DataFrame으로 변환 후 CSV 파일로 저장합니다.
        4. 생성된 CSV 파일을 MinIO 'air-quality' 버킷에 업로드합니다.
    """
    @task
    def extract_and_save_to_minio():

        service_key = Variable.get("AIRKOREA_AIR_QUALITY_SERVICE_KEY")

        # search_date = "2025-08-24"
        yesterday = datetime.now() - timedelta(days=1)
        search_date = yesterday.strftime("%Y-%m-%d")

        url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustFrcstDspth"

        params = {
            "serviceKey" : service_key,
            "returnType" : "json",
            "numOfRows" : 100,
            "pageNo" : 1,
            "searchDate" : search_date,
            "InformCode" : "PM10" # 예보 종류 (PM10 : 미세먼지 , PM25 : 초미세먼지)
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            logging.info(response.text)


            items = response.json().get("response", {}).get("body", {}).get("items", [])
            
            if not items:
                logging.info(f"'{search_date}' 대기오염 데이터가 없습니다.")
                return

            df = pd.DataFrame(items)
            file_path = f"/opt/airflow/data/airkorea_air_quality_{search_date}.csv"

            df.to_csv(file_path, index=False)
            logging.info(f"CSV 파일 저장완료 {file_path}")

            client = Minio(
                "minio:9000",
                access_key="minio",
                secret_key="minio1234",
                secure=False
            )
    
            bucket_name = "airkorea-air-quality"
            object_name = f"raw/airkorea_air_quality_{search_date}.csv"

            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                logging.info(f"Minio 버킷 생성 : {bucket_name}")

            client.fput_object(bucket_name, object_name, file_path)
            logging.info(f"Minio 업로드 완료 : {bucket_name}/{object_name}")


        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 실패 : {e}")
            raise
        except Exception as e:
            logging.error(f"데이터 처리 또는 Minio 업로드 실패: {e}")
            raise

    extract_and_save_to_minio()

AIRKOREA_AIR_QUALITY_PIPELINE()

