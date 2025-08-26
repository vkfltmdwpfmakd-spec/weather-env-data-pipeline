from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


@dag(
    dag_id = "load_gcs_to_bigquery",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup = False,
    tags=["cloud", "gcs", "bigquery", "load"]
)

def load_gcs_to_bigquery_pipeline():
    """
    [클라우드 버전] GCS -> BigQuery 데이터 적재 파이프라인
    - GCS 버킷에 저장된 CSV 파일을 BigQuery Staging 테이블로 적재합니다.
    - Extract DAG들이 먼저 성공하기를 기다립니다.
    """

    gcp_conn_id = "google_cloud_default"
    project_id = "weather-env-data-pipeline"
    staging_dataset = "weather_staging" # BigQuery에 생성한 데이터세트 이름

    yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    yesterday_date_hyphen = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # 단기예보 수집 dag 기다리기
    wait_for_kma_gcs = ExternalTaskSensor(
        task_id = "wait_for_kma_gcs",
        external_dag_id = "KMA_SHORT_FORECAST_PIPELINE_GCS",
        external_task_id = None,
        timeout = 600,
        poke_interval = 30,
        mode = 'poke'
    )

    # 에어코리아 대기오염 수집 dag 기다리기
    wait_for_airkorea_gcs = ExternalTaskSensor(
        task_id = "wait_for_airkorea_gcs",
        external_dag_id = "AIRKOREA_AIR_QUALITY_PIPELINE_GCS",
        external_task_id = None,
        timeout = 600,
        poke_interval = 30,
        mode = 'poke'
    )

    # 단기예보 데이터 gcs에서 bigquery로 로드
    load_kma_to_bq = GCSToBigQueryOperator(
        task_id = "load_kma_to_bq",
        gcp_conn_id = gcp_conn_id,
        bucket = "kma-forecasts-bucket",
        source_objects = [f"raw/kma_short_forecast_{yesterday_date}.csv"],
        destination_project_dataset_table = f"{project_id}.{staging_dataset}.kma_short_forecast_staging",
        write_disposition = "WRITE_TRUNCATE", # 매일 테이블을 새로 씀 (덮어쓰기)
        autodetect = True # 스키마 자동 감지
    )

    # 에어코리아 데이터 gcs에서 bigquery로 로드
    load_airkorea_to_bq = GCSToBigQueryOperator(
        task_id = "load_airkorea_to_bq",
        gcp_conn_id = gcp_conn_id,
        bucket = "airkorea-air-quality-bucket",
        source_objects = [f"raw/airkorea_air_quality_{yesterday_date_hyphen}.csv"],
        destination_project_dataset_table = f"{project_id}.{staging_dataset}.airkorea_air_quality_staging",
        write_disposition = "WRITE_TRUNCATE", # 매일 테이블을 새로 씀 (덮어쓰기)
        autodetect = True # 스키마 자동 감지
    )

    # 의존성 설정
    wait_for_kma_gcs >> load_kma_to_bq
    wait_for_airkorea_gcs >> load_airkorea_to_bq

load_gcs_to_bigquery_pipeline()


