from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import logging
import os

# Great Expectations 연동
# FileDataContext는 great_expectations.yml 파일을 직접 찾아 컨텍스트를 로드
from great_expectations.data_context import FileDataContext


@dag(
    dag_id = "data_quality_check",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags = ["quality", "quality_check", "great_expectations"]
)

def data_quality_check_pipeline():
    """
    데이터 품질 검사(DQ) 파이프라인
    - `load` DAG가 완료되기를 기다립니다.
    - Great Expectations 체크포인트를 실행하여 데이터 품질을 검사합니다.
    - 검사에 실패하면 파이프라인을 중단시킵니다.
    """

    # 이전 단계인 'load' DAG가 성공적으로 완료되기를 기다리는 센서
    wait_for_load_dag = ExternalTaskSensor(
        task_id = "wait_for_load_dag",
        external_dag_id = "load_data_from_minio_to_postgres",
        external_task_id = None, # Dag 전체 성공을 기다림
        timeout = 300,
        poke_interval = 30,
        mode = 'poke'
    )

    @task
    def run_ge_checkpoint():

        # ge 프로젝트 경로 설정 , docker-compse.yml 에서 설정한 경로
        context = FileDataContext.create(project_root_dir="/opt/airflow/notebooks/gx")

        # load Dag가 어재 날짜 처리해서 어제 날짜로 설정
        yesterday_str_ymd = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        yesterday_str_y_m_d = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # 검사 할 데이터 정보
        checks = [
            {
                "checkpoint_name" : "kma_checkpoint",
                "data_asset_name" : f"kma_short_forecast_{yesterday_str_ymd}"
            },
            {
                "checkpoint_name" : "airkorea_checkpoint",
                "data_asset_name" : f"airkorea_air_quality_{yesterday_str_y_m_d}"
            }
        ]

        all_checks_succeeded = True
        for check in checks:
            logging.info(f"--- {check['checkpoint_name']} 시작 ---")

            file_path = f"/opt/airflow/data/{check['data_asset_name']}.csv"

            # 파일 존재 여부 확인
            if not os.path.exists(file_path):
                logging.error(f"파일이 존재하지 않습니다. {file_path}")
                all_checks_succeeded = False
                continue


            # Great Expectations 체크포인트 실행
            """
                runtime_parameters:
                - 실행 시점에 동적으로 결정되는 파라미터
                - batch_data: 검증할 실제 데이터 파일의 전체 경로

                batch_identifiers: 
                - 배치(데이터 묶음)를 식별하기 위한 메타데이터
                - 로그나 결과에서 어떤 데이터를 검증했는지 추적 가능
            """
            try:
                result = context.run_checkpoint(
                    checkpoint_name = check["checkpoint_name"],
                    batch_request = {
                        "runtime_parameters" : {"batch_data" : file_path},
                        "batch_identifiers" : {"default_identifier_name" : check["data_asset_name"]}
                    }
                )
                
                # 성공 여부 확인
                if result["success"]:
                    logging.info(f"--- {check['checkpoint_name']} 성공 ---")
                else:
                    logging.error(f"!!! {check['checkpoint_name']} 실패 !!!")
                    all_checks_succeeded = False
            except Exception as e:
                logging.error(f"체크 포인트 실행 중 오류 발생: {e}")
                all_checks_succeeded = False
                
        # 하나라도 실패하면 전체 태스크를 실패 처리하여 파이프라인을 중단
        if not all_checks_succeeded:
            raise ValueError("데이터 품질 검사 중 하나 이상이 실패했습니다.")

    # 의존성 설정
    wait_for_load_dag >> run_ge_checkpoint()

data_quality_check_pipeline()