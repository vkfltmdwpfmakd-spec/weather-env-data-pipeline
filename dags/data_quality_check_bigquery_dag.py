from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import logging

# Great Expectations 연동
from great_expectations.data_context import FileDataContext

@dag(
    dag_id="data_quality_check_bigquery",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["cloud", "quality_check", "great_expectations", "bigquery"],
)
def data_quality_check_bigquery_pipeline():
    """
    ### [클라우드 버전] BigQuery 데이터 품질 검사(DQ) 파이프라인
    - `load_gcs_to_bigquery` DAG가 완료되기를 기다립니다.
    - Great Expectations 체크포인트를 실행하여 BigQuery Staging 테이블의 품질을 검사합니다.
    """
    
    wait_for_load_dag = ExternalTaskSensor(
        task_id="wait_for_load_gcs_to_bigquery",
        external_dag_id="load_gcs_to_bigquery",
        external_task_id=None,
        timeout=600,
        poke_interval=30,
        mode='poke'
    )

    @task
    def run_ge_bigquery_checkpoints():
        """Great Expectations 체크포인트를 실행하여 BigQuery 테이블을 검사합니다."""
        import os
        
        # GCP 인증 설정
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/gcp-keys/weather-env-data-pipeline-4e8d048be7a7.json'
        
        # 작업 디렉토리를 Great Expectations 프로젝트 루트로 변경
        original_cwd = os.getcwd()
        os.chdir("/opt/airflow/gx")
        
        try:
            logging.info(f"현재 작업 디렉토리: {os.getcwd()}")
            
            # Great Expectations 디렉토리 권한 문제 해결을 위한 임시 디렉토리 생성 시도
            try:
                import stat
                os.makedirs("/opt/airflow/gx/uncommitted/validations/kma_data_rules", mode=0o777, exist_ok=True)
                os.makedirs("/opt/airflow/gx/uncommitted/validations/airkorea_data_rules", mode=0o777, exist_ok=True)
            except PermissionError:
                logging.warning("디렉토리 생성 권한 없음 - 체크포인트 실행 시 임시 파일로 처리")
            
            # project_root_dir 없이 FileDataContext 생성 (현재 디렉토리 사용)
            context = FileDataContext()
            
            checkpoints_to_run = ["kma_bq_checkpoint", "airkorea_bq_checkpoint"]
            all_succeeded = True

            for checkpoint_name in checkpoints_to_run:
                logging.info(f"--- {checkpoint_name} 실행 시작 ---")
                result = context.run_checkpoint(checkpoint_name=checkpoint_name)
                if not result["success"]:
                    logging.warning(f"⚠️ {checkpoint_name} 품질 검사에서 일부 규칙 실패 (파이프라인 계속 진행)")
                    all_succeeded = False
                else:
                    logging.info(f"✅ {checkpoint_name} 품질 검사 성공!")
            
            # 품질 검사 결과를 로그로만 기록하고 파이프라인 계속 진행
            if not all_succeeded:
                logging.warning("⚠️ 일부 품질 검사 규칙이 실패했지만 데이터 파이프라인을 계속 진행합니다.")
                
        finally:
            # 원래 작업 디렉토리로 복원
            os.chdir(original_cwd)

    wait_for_load_dag >> run_ge_bigquery_checkpoints()

data_quality_check_bigquery_pipeline()