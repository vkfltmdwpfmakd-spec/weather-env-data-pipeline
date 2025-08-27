from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

@dag(
    dag_id = "transform_data_with_dbt",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup = False,
    tags = ["transform", "dbt"]
)

def transform_data_with_dbt_pipeline():
    """
        dbt 변환(Transform) 파이프라인
        - 이 DAG는 `dbt run` 명령어를 실행하여 Staging 테이블의 데이터를 최종 분석용 테이블(Analytics)로 변환
        - `data_quality_check` DAG가 성공적으로 완료된 후에 실행되어야 함
    """

    wait_for_quality_check_dag = ExternalTaskSensor(
        task_id = "wait_for_quality_check_dag",
        external_dag_id = "data_quality_check",
        external_task_id = None,
        timeout = 600,
        poke_interval = 30,
        mode = 'poke'
    )

    dbt_run_task = BashOperator(
        task_id = "run_dbt_models",
        bash_command = """
        # dbt profiles.yml 생성
        mkdir -p /home/airflow/.dbt
        cat > /home/airflow/.dbt/profiles.yml << 'EOF'
        weather_project:
        target: dev
        outputs:
            dev:
            type: postgres
            host: postgres
            user: airflow
            password: airflow
            port: 5432
            dbname: airflow
            schema: analytics
            threads: 1
        EOF
        
        # dbt 실행 (PostgreSQL 전용 모델만)
        cd /opt/airflow/dbt/weather_project && dbt run --select daily_air_quality_forecast daily_short_forecast_summary --log-path /tmp/dbt_logs
        """
    )

    # 의존성 설정
    wait_for_quality_check_dag >> dbt_run_task

transform_data_with_dbt_pipeline()  # DAG 실행
