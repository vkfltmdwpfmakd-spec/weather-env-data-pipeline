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
from sqlalchemy import create_engine # DB연결 위한

@dag(
    dag_id = "load_data_from_minio_to_postgres",
    schedule_interval="@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup = False,
    tags = ["data", "load", "postgres", "minio"]
)

def load_data_from_minio_to_postgres():
    """
        MinIO -> Postgres 데이터 적재 파이프라인
        1. MinIO의 각 버킷에서 어제 날짜의 CSV 파일을 찾습니다.
        2. CSV 파일을 Pandas DataFrame으로 읽어옵니다.
        3. DataFrame을 PostgreSQL의 Staging 테이블로 적재합니다.
    """

    # Minio 접속정보
    MINIO_CLIENT = Minio(
        "minio:9000",
        access_key = "minio",
        secret_key = "minio1234",
        secure = False
    )

    # Postgres 접속정보
    POSTGRES_CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

    # 어제 날짜
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    @task
    def load_short_forecast_data():
        
        bucket_name = "kma-short-forecast"
        object_name = f"raw/kma_short_forecast_{yesterday_str}.csv"
        local_file_path = f"/opt/airflow/data/kma_short_forecast_to_load.csv"
        table_name = "kma_short_forecast_staging"

        try:
            # Minio에서 파일 다운로드
            MINIO_CLIENT.fget_object(bucket_name, object_name, local_file_path)

            # CSV파일 DataFrame 으로 읽기
            df = pd.read_csv(local_file_path)
            logging.info(f"{table_name} 데이터 프레임 생성 완료, 행: {len(df)}")

            # Postgres에 저장
            engine = create_engine(POSTGRES_CONN_STR)
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            logging.info(f"{table_name} 데이터 적재 완료")

            # 로컬 파일 삭제
            os.remove(local_file_path)


        except Exception as e:
            logging.error(f"데이터 적재 실패: {e}")
            raise

    @task
    def load_air_quality_data():

        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        bucket_name = "airkorea-air-quality"
        object_name = f"raw/airkorea_air_quality_{yesterday_str}.csv"
        local_file_path = f"/opt/airflow/data/airkorea_air_quality_to_load.csv"
        table_name = "airkorea_air_quality_staging"

        logging.info(f"오브젝트이름 : {object_name}")
        try:
            # Minio에서 파일 다운로드
            MINIO_CLIENT.fget_object(bucket_name, object_name, local_file_path)

            # CSV파일 DataFrame 으로 읽기
            df = pd.read_csv(local_file_path)
            logging.info(f"{table_name} 데이터 프레임 생성 완료, 행: {len(df)}")

            # Postgres에 저장
            engine = create_engine(POSTGRES_CONN_STR)
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            logging.info(f"{table_name} 데이터 적재 완료")

            # 로컬 파일 삭제
            os.remove(local_file_path)


        except Exception as e:
            logging.error(f"데이터 적재 실패: {e}")
            raise


    # 병렬 실행
    load_short_forecast_data()
    load_air_quality_data()
    
load_data_from_minio_to_postgres()
        





