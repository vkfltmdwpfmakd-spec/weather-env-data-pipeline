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
    dag_id = "AIRKOREA_AIR_QUALITY_PIPELINE_GCS",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags = ["cloud", "gcp", "weather", "airkorea"]
)

def AIRKOREA_AIR_QUALITY_PIPELINE_GCS():

    @task
    def extract_and_save_to_gcs():

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

            gcp_conn_id = "google_cloud_default"
            bucket_name = "airkorea-air-quality-bucket"
            object_name = f"raw/airkorea_air_quality_{search_date}.csv"

            gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=file_path
            )

            logging.info(f"GCS 업로드완료 : gs://{bucket_name}/{object_name}")
            os.remove(file_path)

        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 실패 : {e}")
            raise
        except Exception as e:
            logging.error(f"데이터 처리 또는 GCS 업로드 실패: {e}")
            raise

    extract_and_save_to_gcs()

AIRKOREA_AIR_QUALITY_PIPELINE_GCS()