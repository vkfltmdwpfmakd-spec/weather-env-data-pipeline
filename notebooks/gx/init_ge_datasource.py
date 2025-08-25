{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "1ad7be53-63cf-486e-b85f-c9057e611fb9",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<great_expectations.datasource.new_datasource.Datasource at 0x7d7fd3242ad0>"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "from great_expectations.data_context import DataContext\n",
    "\n",
    "context = DataContext(\"/home/jovyan/notebooks/gx\")  # gx 디렉토리 경로 맞게 넣기\n",
    "\n",
    "datasource_config = {\n",
    "    \"name\": \"airflow_data\",\n",
    "    \"class_name\": \"Datasource\",\n",
    "    \"execution_engine\": {\"class_name\": \"PandasExecutionEngine\"},\n",
    "    \"data_connectors\": {\n",
    "        \"default_inferred_data_connector_name\": {\n",
    "            \"class_name\": \"InferredAssetFilesystemDataConnector\",\n",
    "            \"base_directory\": \"/opt/airflow/data\",\n",
    "            \"default_regex\": {\n",
    "                \"pattern\": \"(.*)\\\\.csv\",\n",
    "                \"group_names\": [\"data_asset_name\"],\n",
    "            },\n",
    "        },\n",
    "    },\n",
    "}\n",
    "\n",
    "context.add_datasource(**datasource_config)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "9d3914a0-c542-47d3-912f-68f6ca7ff5ff",
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
