{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "02fcfac5-9b45-46f1-ac10-d77742eca421",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "INFO:root:'kma_checkpoint'와 'airkorea_checkpoint'를 성공적으로 생성/업데이트했습니다.\n"
     ]
    }
   ],
   "source": [
    "import great_expectations as gx\n",
    "import logging\n",
    "\n",
    "# 1. Great Expectations 데이터 컨텍스트(프로젝트)를 불러옵니다.\n",
    "# great_expectations.yml 파일이 있는 경로를 정확히 지정해야 합니다.\n",
    "context = gx.get_context(context_root_dir=\"/home/jovyan/notebooks/gx\")\n",
    "\n",
    "# 2. 체크포인트 설정을 위한 딕셔너리를 정의합니다.\n",
    "kma_checkpoint_config = {\n",
    "    \"name\": \"kma_checkpoint\",\n",
    "    \"config_version\": 1.0,\n",
    "    \"class_name\": \"SimpleCheckpoint\",\n",
    "    \"run_name_template\": \"%Y%m%d-%H%M%S-kma-run\",\n",
    "    \"validations\": [\n",
    "        {\n",
    "            \"batch_request\": {\n",
    "                \"datasource_name\": \"airflow_data\",\n",
    "                \"data_connector_name\": \"default_inferred_data_connector_name\",\n",
    "                # 실제 검사는 Airflow DAG가 실행될 때 동적으로 파일 이름을 지정하므로\n",
    "                # 여기서는 임의의 플레이스홀더 이름을 사용합니다.\n",
    "                \"data_asset_name\": \"kma_short_forecast_placeholder\",\n",
    "            },\n",
    "            \"expectation_suite_name\": \"kma_data_rules\",\n",
    "        }\n",
    "    ],\n",
    "}\n",
    "\n",
    "airkorea_checkpoint_config = {\n",
    "    \"name\": \"airkorea_checkpoint\",\n",
    "    \"config_version\": 1.0,\n",
    "    \"class_name\": \"SimpleCheckpoint\",\n",
    "    \"run_name_template\": \"%Y%m%d-%H%M%S-airkorea-run\",\n",
    "    \"validations\": [\n",
    "        {\n",
    "            \"batch_request\": {\n",
    "                \"datasource_name\": \"airflow_data\",\n",
    "                \"data_connector_name\": \"default_inferred_data_connector_name\",\n",
    "                \"data_asset_name\": \"airkorea_air_quality_placeholder\",\n",
    "            },\n",
    "            \"expectation_suite_name\": \"airkorea_data_rules\",\n",
    "        }\n",
    "    ],\n",
    "}\n",
    "\n",
    "# 3. 컨텍스트에 체크포인트 설정을 추가하거나 업데이트합니다.\n",
    "context.add_or_update_checkpoint(**kma_checkpoint_config)\n",
    "context.add_or_update_checkpoint(**airkorea_checkpoint_config)\n",
    "\n",
    "logging.basicConfig(level=logging.INFO)\n",
    "logging.info(\"'kma_checkpoint'와 'airkorea_checkpoint'를 성공적으로 생성/업데이트했습니다.\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "1e9477f6-65c3-446c-8f0e-bc4e7f75b7d7",
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
