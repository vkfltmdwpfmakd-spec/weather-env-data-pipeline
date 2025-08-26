{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "d8fded89-80bb-4a76-80ab-dc28f00963ba",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "INFO:root:BigQuery 데이터 소스 및 체크포인트를 성공적으로 생성/업데이트했습니다.\n"
     ]
    }
   ],
   "source": [
    "import great_expectations as gx\n",
    "import logging\n",
    "import os\n",
    "import glob\n",
    "\n",
    "\n",
    "# 서비스 계정 키 파일 경로를 환경변수로 설정\n",
    "os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jovyan/gcp-keys/weather-env-data-pipeline-4e8d048be7a7.json'\n",
    "\n",
    "# 1. Great Expectations 데이터 컨텍스트를 불러옵니다.\n",
    "context = gx.get_context(context_root_dir=\"/home/jovyan/notebooks/gx\")\n",
    "\n",
    "# 2. BigQuery 데이터 소스 설정\n",
    "datasource_name = \"bigquery_datasource\"\n",
    "project_id = \"weather-env-data-pipeline\"\n",
    "dataset_name = \"weather_staging\" # Staging 데이터세트 이름\n",
    "\n",
    "# BigQuery 접속을 위한 connection string을 만듭니다.\n",
    "connection_string = f\"bigquery://{project_id}/{dataset_name}\"\n",
    "\n",
    "# [수정] 범용 SQL 데이터 소스 추가 함수인 add_sql을 사용합니다.\n",
    "datasource = context.sources.add_sql(\n",
    "    name=datasource_name,\n",
    "    connection_string=connection_string,\n",
    ")\n",
    "\n",
    "# 3. BigQuery용 체크포인트 생성\n",
    "# (기존에 만든 Expectation Suite(검사 규칙)를 그대로 재사용합니다)\n",
    "kma_checkpoint_config = {\n",
    "    \"name\": \"kma_bq_checkpoint\",\n",
    "    \"validations\": [\n",
    "        {\n",
    "            \"batch_request\": {\n",
    "                \"datasource_name\": datasource_name,\n",
    "                \"data_asset_name\": \"kma_short_forecast_staging\", # BigQuery 테이블 이름\n",
    "            },\n",
    "            \"expectation_suite_name\": \"kma_data_rules\",\n",
    "        }\n",
    "    ],\n",
    "}\n",
    "\n",
    "airkorea_checkpoint_config = {\n",
    "    \"name\": \"airkorea_bq_checkpoint\",\n",
    "    \"validations\": [\n",
    "        {\n",
    "            \"batch_request\": {\n",
    "                \"datasource_name\": datasource_name,\n",
    "                \"data_asset_name\": \"airkorea_air_quality_staging\", # BigQuery 테이블 이름\n",
    "            },\n",
    "            \"expectation_suite_name\": \"airkorea_data_rules\",\n",
    "        }\n",
    "    ],\n",
    "}\n",
    "\n",
    "context.add_or_update_checkpoint(**kma_checkpoint_config)\n",
    "context.add_or_update_checkpoint(**airkorea_checkpoint_config)\n",
    "\n",
    "logging.basicConfig(level=logging.INFO)\n",
    "logging.info(\"BigQuery 데이터 소스 및 체크포인트를 성공적으로 생성/업데이트했습니다.\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "b660ee81-82a7-4c11-9b35-eba7367a8f24",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
