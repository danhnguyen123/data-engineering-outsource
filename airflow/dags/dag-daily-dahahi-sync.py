from datetime import timedelta
from datetime import datetime
import pendulum
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.config import config

from plugins.helper.logging_helper import LoggingHelper
from plugins.helper.redis_helper import RedisHelper
from plugins.helper.airflow_helper import on_failure_callback_discord
from plugins.helper.gcp_helper import BQHelper
from plugins.helper.dahahi_helper import DahahiHelper

from plugins.modules import (
    DahahiEmployeesETL,
)

DAHAHI_NAMESPACE = 'dahahi'
EMPLOYEES = 'employees'

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = LoggingHelper.get_configured_logger(DAHAHI_NAMESPACE)
redis = RedisHelper(logger=logger, redis_host=config.REDIS_HOST, redis_port=config.REDIS_PORT)
dahahi = DahahiHelper(logger=logger)
bq = BQHelper(logger=logger)

mapping_etl = {
    EMPLOYEES: DahahiEmployeesETL,
}

mapping_client = {
    DAHAHI_NAMESPACE: dahahi,
}

dahahi_tables_list = [EMPLOYEES]

default_args = {
    'owner': 'danh.nguyen',
    'depends_on_past': False,
    'on_failure_callback': on_failure_callback_discord,
    'trigger_rule' : 'all_done', #https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['de@datalize.cloud'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
}

def call_python_etl(namespace, table_name, task_name, **kwargs):

    table_config = kwargs['dag_run'].conf.get(table_name) if kwargs['dag_run'].conf.get(table_name) else kwargs['params'].get(table_name)

    run = table_config.get(task_name)

    table_etl = mapping_etl.get(table_name)(
        logger=logger,
        project_id=config.PROJECT_ID,
        dataset_id=config.DATASET_ID,
        table_name=table_name,
        api_client=mapping_client.get(namespace),
        bq=bq,
        namespace=namespace,
        kwargs=kwargs
    )

    return getattr(table_etl, task_name)(run=run)


def contruct_params():
    params={}
    for table in dahahi_tables_list:
        params.update({
            table: {
                "extract": True,
                "load": True
            }
        })
    return params

with DAG(
    'dag-daily-dahahi-sync',
    default_args=default_args,
    description='ETL Employee list from Dahahi to BigQuery',
    schedule_interval='0 7 * * *' if config.ENV == "prod" else None,
    start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
    catchup=False,
    tags=['daily','elt','dahahi', 'bigquery'],
    params=contruct_params(),
    dag_display_name="ETL Employee list from Dahahi daily at 7AM"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup(DAHAHI_NAMESPACE, tooltip=f"Update all thing for {DAHAHI_NAMESPACE}") as dahahi_etl_group:

        for table in dahahi_tables_list:

            with TaskGroup(table, tooltip=f"Update all thing for {table}") as table_group:

                extract = PythonOperator(
                    task_id=f"extract_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    op_kwargs={
                        "namespace": DAHAHI_NAMESPACE,
                        "table_name": table,
                        "task_name": "extract"
                        }
                    )

                load = PythonOperator(
                    task_id= f"load_{table}",
                    python_callable=call_python_etl,
                    provide_context=True,
                    op_kwargs={
                        "namespace": DAHAHI_NAMESPACE,
                        "table_name": table,
                        "task_name": "load"
                        }
                    )

                extract >> load

    start >> dahahi_etl_group >> end
