from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

@dag(
    dag_id="transform_dbt_bigquery",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["cloud", "dbt", "bigquery", "transform"]
)
def transform_dbt_bigquery_pipeline():
    
    wait_for_quality_check_bq_dag = ExternalTaskSensor(
        task_id="wait_for_quality_check_bq_dag",
        # BigQuery 품질 검사 DAG를 기다리도록 변경
        external_dag_id="data_quality_check_bigquery",
        external_task_id=None,
        timeout = 600,
        poke_interval = 30,
        mode = 'poke'
    )
    
    
    dbt_run_bigquery_task = BashOperator(
        task_id="run_dbt_bigquery_models",
        # --profiles-dir 옵션으로 BigQuery용 프로필 폴더를 지정합니다.
        # dbt 프로젝트 폴더와 profiles_bigquery 폴더를 모두 Airflow에 마운트해야 합니다.
        bash_command="cd /opt/airflow/dbt/weather_project && dbt run --profiles-dir ../../dbt_profiles/bigquery"
    )
    
    # 의존성 설정
    wait_for_quality_check_bq_dag >> dbt_run_bigquery_task

transform_dbt_bigquery_pipeline()