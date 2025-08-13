from datetime import timedelta
from datetime import datetime
import pendulum
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.redis_helper import RedisHelper
from plugins.helper.mongodb_helper import MongoDBHeler
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.gcp_helper import GCSHelper, BQHelper
import plugins.helper.time_helper as TimeHelper  
import envs.env as env
from plugins.modules.gsheet.ttc_gsheet import (
    etl_ttc_survey,
    etl_ttc_external_facebook
)


NAMESPACE = 'gsheet'

TTC_SURVEY = 'ttc_survey'
TTC_EXTERNAL_FACEBOOK = 'ttc_external_facebook'

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(__name__)
# bq = BQHelper(logger=logger)

mapping_etl = {
    TTC_SURVEY: etl_ttc_survey,
    TTC_EXTERNAL_FACEBOOK: etl_ttc_external_facebook
}

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    # 'on_retry_callback': on_failure_callback_discord,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1 if config.ENV == "prod" else 1,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}


with DAG(
    'dag-daily-ttc-gsheet',
    default_args=default_args,
    description='ETL Data from GSheet to BigQuery',
    schedule_interval='0 1 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=["daily", 'elt','gsheet', 'bigquery'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup(NAMESPACE, tooltip=f"Update all thing for {NAMESPACE}") as etl_group:

        for table in mapping_etl:

            extract = PythonOperator(
                task_id=f"extract_{table}",
                python_callable=mapping_etl.get(table),
                provide_context=True,
                )
 
    start >> etl_group >> end

            
