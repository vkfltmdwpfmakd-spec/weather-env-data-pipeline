from __future__ import annotations
import os
import pendulum
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import logging

@dag(
    dag_id = "KMA_SHORT_FORECAST_PIPELINE_GCS",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup = False,
    tags = ["cloud", "gcp", "weather, kma, forecast"]
)

def kma_short_forecast_pipeline_gcs():
    
    @task
    def extract_and_save_to_gcs():
        service_key = Variable.get("KMA_SHORT_FORECAST_KEY") # API KEY 설정

        # base_date = "20250825"
        yesterday = datetime.now() - timedelta(days=1)
        base_date = yesterday.strftime("%Y%m%d") # 어제 날짜
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

            gcp_conn_id = "google_cloud_default"
            bucket_name = "kma-forecasts-bucket" # 데이터를 저장할 버킷 이름
            object_name = f"raw/kma_short_forecast_{base_date}.csv" # 버킷 내에 저장될 파일 경로와 이름

            gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=file_path,
            )

            logging.info(f"GCS 업로드완료 : gs://{bucket_name}/{object_name}")
            os.remove(file_path)

        # 에러 관련 사항들을 세세하게 나눠서 관리 할 수 있도록 할 예정 [디버깅]
        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 오류: {e}")
            raise # 에러를 다시 발생시켜 Airflow가 태스크를 실패 처리하도록 함
        except Exception as e:
            logging.error(f"데이터 처리 또는 GCS 업로드 실패: {e}")
            raise
    
    extract_and_save_to_gcs()

kma_short_forecast_pipeline_gcs()


    